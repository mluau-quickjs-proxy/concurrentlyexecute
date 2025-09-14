use std::{path::PathBuf, sync::Arc, time::Duration};

use remoc::rch::mpsc::Sender;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::{UnboundedReceiver, UnboundedSender}, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio::process::Child;

pub type BaseError = Box<dyn std::error::Error + Send + Sync>;

/// A guard that deletes a temporary file on drop
/// 
/// This is internal and used by concurrentlyexec to try and delete unix domain sockets 
/// after use
pub struct TempFileGuard {
    path: PathBuf,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Options for creating a new thread
pub struct ProcessOpts {
    /// Must contain the executable path as the first argument
    /// or `-` to use the current executable
    /// 
    /// ConcurrentExecutor will automatically set the needed env variables
    /// 
    /// The process must then call ConcurrentExecutor::run_process_client to connect
    /// to the parent process
    pub cmd_argv: Vec<String>,
    pub cmd_envs: Vec<(String, String)>,
    pub mem_soft_limit: i64,
    pub mem_hard_limit: i64,
    pub start_timeout: Duration
}

impl ProcessOpts {
    pub fn new(cmd_argv: Vec<String>) -> Self {
        Self {
            cmd_argv,
            cmd_envs: vec![],
            mem_soft_limit: 0,
            mem_hard_limit: 0,
            start_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_envs(mut self, envs: Vec<(String, String)>) -> Self {
        self.cmd_envs = envs;
        self
    }

    pub fn with_mem_limits(mut self, soft_limit: i64, hard_limit: i64) -> Self {
        self.mem_soft_limit = soft_limit;
        self.mem_hard_limit = hard_limit;
        self
    }

    pub fn with_start_timeout(mut self, timeout: Duration) -> Self {
        self.start_timeout = timeout;
        self
    }
}

/// A oneshot sender that can be either local or process
/// 
/// This should be used in place of tokio oneshot/remoc directly
pub enum OneshotSender<T> {
    Local {
        sender: tokio::sync::oneshot::Sender<T>,
    },
    Process {
        sender: remoc::rch::oneshot::Sender<T>,
    },
}

impl<T: Serialize + DeserializeOwned + Send + 'static> OneshotSender<T> {
    /// Creates a new local oneshot sender
    pub fn new_local(sender: tokio::sync::oneshot::Sender<T>) -> Self {
        Self::Local { sender }
    }

    /// Creates a new process oneshot sender
    pub fn new_process(sender: remoc::rch::oneshot::Sender<T>) -> Self {
        Self::Process { sender }
    }

    /// Sends a value through the oneshot sender
    pub fn send(self, value: T) -> Result<(), BaseError> {
        match self {
            Self::Local { sender } => {
                let _ = sender.send(value);
            }
            Self::Process { sender } => {
                match sender.send(value) {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(format!("Failed to send value through process oneshot sender: {}", e.to_string()).into());
                    }
                }
            }
        }
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Serialize for OneshotSender<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Local { .. } => {
                Err(serde::ser::Error::custom("Cannot serialize local oneshot sender"))
            }
            Self::Process { sender } => {
                // Serialize as [0, sender]
                sender.serialize(serializer)
            }
        }
    }
}

impl<'de, T: Serialize + DeserializeOwned + Send + 'static> Deserialize<'de> for OneshotSender<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let sender = remoc::rch::oneshot::Sender::<T>::deserialize(deserializer)?;
        Ok(Self::Process { sender })
    }
}

pub enum OneshotReceiver<T> {
    Local {
        receiver: tokio::sync::oneshot::Receiver<T>,
    },
    Process {
        receiver: remoc::rch::oneshot::Receiver<T>,
    },
}

impl<T: Serialize + DeserializeOwned + Send + 'static> OneshotReceiver<T> {
    /// Creates a new local oneshot receiver
    pub fn new_local(receiver: tokio::sync::oneshot::Receiver<T>) -> Self {
        Self::Local { receiver }
    }

    /// Creates a new process oneshot receiver
    pub fn new_process(receiver: remoc::rch::oneshot::Receiver<T>) -> Self {
        Self::Process { receiver }
    }

    /// Receives a value from the oneshot receiver
    pub async fn recv(self) -> Result<T, BaseError> {
        match self {
            Self::Local { receiver } => {
                match receiver.await {
                    Ok(value) => Ok(value),
                    Err(e) => Err(format!("Failed to receive value from local oneshot receiver: {}", e.to_string()).into()),
                }
            }
            Self::Process { receiver } => {
                match receiver.await {
                    Ok(value) => Ok(value),
                    Err(e) => Err(format!("Failed to receive value from process oneshot receiver: {}", e.to_string()).into()),
                }
            }
        }
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Serialize for OneshotReceiver<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Local { .. } => {
                Err(serde::ser::Error::custom("Cannot serialize local oneshot receiver"))
            }
            Self::Process { receiver } => {
                // Serialize as [0, receiver]
                receiver.serialize(serializer)
            }
        }
    }
}

impl<'de, T: Serialize + DeserializeOwned + Send + 'static> Deserialize<'de> for OneshotReceiver<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let receiver = remoc::rch::oneshot::Receiver::<T>::deserialize(deserializer)?;
        Ok(Self::Process { receiver })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum ProcessMessage<T: ConcurrentlyExecute> {
    Message {
        data: Message<T>,
    },
    LoadMpsc {
        recv: remoc::rch::mpsc::Receiver<ProcessMessage<T>>,
        recieved: remoc::rch::oneshot::Sender<()>,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Message<T: ConcurrentlyExecute> {
    Data {
        data: T::Message,
    },
    Shutdown,
}

#[allow(async_fn_in_trait)]
/// Trait for types that can be executed concurrently
pub trait ConcurrentlyExecute: Send + Sync + Clone + Sized + 'static {
    type Message: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static;

    async fn run(
        rx: UnboundedReceiver<Message<Self>>, 
    );
}

/// State for a concurrent executor
#[derive(Clone)]
pub struct ConcurrentExecutorState<T: ConcurrentlyExecute> {
    pub cancel_token: CancellationToken,
    pub sema: Arc<Semaphore>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: ConcurrentlyExecute> ConcurrentExecutorState<T> {
    /// Creates a new concurrent executor state
    /// with N permits
    pub fn new(
        max: usize,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        Self::new_with(cancel_token, Arc::new(Semaphore::new(max)))
    }

    /// Creates a new concurrent executor state
    /// with a custom semaphore set
    pub fn new_with(
        cancel_token: CancellationToken,
        sema: Arc<Semaphore>,
    ) -> Self {
        Self {
            cancel_token,
            sema,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Concurrent tokio execution
///
/// Assumes use of local tokio runtime
pub enum ConcurrentExecutor<T: ConcurrentlyExecute> {
    Local {
        state: ConcurrentExecutorState<T>,
        message_tx: UnboundedSender<Message<T>>,
        permit: OwnedSemaphorePermit,
    },
    Threaded {
        state: ConcurrentExecutorState<T>,
        message_tx: UnboundedSender<Message<T>>,
        permit: OwnedSemaphorePermit,
    },
    Process {
        state: ConcurrentExecutorState<T>,
        message_tx: Sender<ProcessMessage<T>>,
        proc_handle: Arc<RwLock<Child>>,
        permit: OwnedSemaphorePermit,
        uds_path: TempFileGuard, // On drop, this destroys the file

        // Not used, but we need to keep them around
        tx: remoc::rch::base::Sender<ProcessMessage<T>, remoc::codec::PostbagSlim>,
        rx: remoc::rch::base::Receiver<ProcessMessage<T>, remoc::codec::PostbagSlim>
    },
}

impl<T: ConcurrentlyExecute> Drop for ConcurrentExecutor<T> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl<T: ConcurrentlyExecute> ConcurrentExecutor<T> {
    /// Creates a new local concurrent executor
    /// that runs in the current thread
    /// 
    /// Asserts that it is being used in a local tokio runtime or LocalSet
    pub async fn new_local(
        cs_state: ConcurrentExecutorState<T>,
    ) -> Result<Self, BaseError> {
        let permit = cs_state.sema.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        
        tokio::task::spawn_local(async move {
            T::run(rx).await;
        });

        Ok(Self::Local {
            state: cs_state,
            message_tx: tx,
            permit,
        })
    }

    /// Creates a new threaded concurrent executor
    /// that runs in a separate thread
    pub async fn new_threaded(
        cs_state: ConcurrentExecutorState<T>,
    ) -> Result<Self, BaseError> {
        let permit = cs_state.sema.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime");
            runtime.block_on(async move {
                let _ = start_tx.send(());
                T::run(rx).await;
            });
        });
        // Wait for the thread to start
        let _waited = start_rx.await?;

        Ok(Self::Threaded {
            state: cs_state,
            message_tx: tx,
            permit,
        })
    }

    /// Creates a new process concurrent executor
    /// that runs in a separate process
    pub async fn new_process(
        cs_state: ConcurrentExecutorState<T>,
        mut opts: ProcessOpts,
    ) -> Result<Self, BaseError> {
        let permit = cs_state.sema.clone().acquire_owned().await?;
        // Create a unix socket pair for communication
        let rand_str = {
            use rand::Rng;

            fn generate_random_alphanumeric_string(length: usize) -> String {
                const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                let mut rng = rand::rng();
                let random_string: String = (0..length)
                    .map(|_| {
                        let idx = rng.random_range(0..CHARSET.len());
                        CHARSET[idx] as char
                    })
                    .collect();
                random_string
            }

            generate_random_alphanumeric_string(16)
        };
        let pbuf = std::env::temp_dir().join(format!("ce{rand_str}.sock"));
        let uds_path = TempFileGuard { path: pbuf.clone() };
        let unix_listener = tokio::net::UnixListener::bind(&uds_path.path)?;

        // Spawn the process here
        // Note that we do not touch stdout/stdin/stderr as the child process
        // should inherit those from the parent
        let exe = {
            if opts.cmd_argv.len() == 0 {
                return Err("cmd_argv must contain at least the executable path".into());
            }
            if opts.cmd_argv[0] == "-" {
                std::env::current_exe()?.to_string_lossy().into_owned()
            } else {
                std::mem::take(&mut opts.cmd_argv[0])
            }
        };
        let args = opts.cmd_argv[1..].to_vec();
        let mut env = vec![
            (
                "CONCURRENT_EXECUTOR_UDS_PATH",
                uds_path.path.as_os_str().to_str().ok_or("Failed to convert UDS path to str")?,
            ),
        ];
        for (k, v) in opts.cmd_envs.iter() {
            env.push((k.as_str(), v.as_str()));
        }
        let mut cmd = tokio::process::Command::new(exe);
        cmd.args(args);
        cmd.envs(env);
        cmd.kill_on_drop(true);
        let mut proc_handle = cmd.spawn()?;

        // Set cgroups up if on unix and memory limits are set
        let mut _guard = None;
        #[cfg(unix)]
        {
            struct CgroupWithDtor {
                _cgroup: cgroups_rs::fs::Cgroup,
            }

            impl Drop for CgroupWithDtor {
                fn drop(&mut self) {
                    match self._cgroup.delete() {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("Failed to delete cgroup: {}", e);
                        }
                    };
                }
            }
            
            if opts.mem_hard_limit > 0 || opts.mem_soft_limit > 0 {
                // Create a cgroup and set memory limits
                let cg = cgroups_rs::fs::cgroup_builder::CgroupBuilder::new(&format!("ce-{}-{}", std::process::id(), rand::random::<u64>()))
                .memory()
                    .memory_soft_limit(opts.mem_soft_limit)
                    .memory_hard_limit(opts.mem_hard_limit)
                .done()
                .build(Box::new(cgroups_rs::fs::hierarchies::V2::new()))
                .expect("Failed to create cgroup");
                assert!(cg.exists());
                cg.add_task(cgroups_rs::CgroupPid::from(proc_handle.id().ok_or("Failed to get child process pid")? as u64))
                    .expect("Failed to add process to cgroup");
                _guard = Some(CgroupWithDtor { _cgroup: cg });
            }
        }
        #[cfg(not(unix))]
        {
            _guard = Some(()); // Dummy guard
        }
        
        // Wait for a connection
        let timer = tokio::time::sleep(opts.start_timeout);
        let (stream, _addr) = tokio::select! {
            _ = timer => {
                return Err("Timed out waiting for process to connect".into());
            }
            res = unix_listener.accept() => res?,
            _ = proc_handle.wait() => {
                cs_state.cancel_token.cancel();
                return Err("Process exited before connecting".into());
            }
        };

        // Create remoc channel
        let (reader, writer) = stream.into_split();
        let (conn, mut tx, rx) = remoc::Connect::io::<tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf, ProcessMessage<T>, ProcessMessage<T>, remoc::codec::PostbagSlim>(
            remoc::Cfg::balanced(), 
            reader, 
            writer, 
        )
            .await
            .map_err(|e| format!("Failed to create remoc connection: {}", e.to_string()))?;
        
        let cancel_token = cs_state.cancel_token.clone();
        let fut = async move { 
            tokio::select! {
                _ = cancel_token.cancelled() => {}
                _ = conn => {
                }
            }
        };

        tokio::task::spawn(fut);

        let (message_tx, message_rx) = remoc::rch::mpsc::channel::<ProcessMessage<T>, _>(256);
        let (conn_finished_tx, conn_finished_rx) = remoc::rch::oneshot::channel();
        tx.send(ProcessMessage::LoadMpsc { recv: message_rx, recieved: conn_finished_tx }).await.map_err(|e| format!("Failed to send LoadMpsc message to process: {}", e.to_string()))?;
        // Wait for the process to acknowledge receiving the LoadMpsc message
        let _ = tokio::time::timeout(opts.start_timeout, conn_finished_rx).await
            .map_err(|_| "Timed out waiting for process to acknowledge LoadMpsc message")?
            .map_err(|e| format!("Failed to receive acknowledgement from process: {}", e.to_string()))?;

        Ok(Self::Process {
            state: cs_state,
            message_tx,
            proc_handle: Arc::new(RwLock::new(proc_handle)),
            permit,
            uds_path,
            tx,
            rx,
        })
    }

    /// Starts up a process client
    /// The process should have first setup a tokio localruntime first
    pub async fn run_process_client() {
        let uds_path = std::env::var("CONCURRENT_EXECUTOR_UDS_PATH").expect("CONCURRENT_EXECUTOR_UDS_PATH not set");
        let stream = tokio::net::UnixStream::connect(uds_path).await.expect("Failed to connect to UDS path");

        let (reader, writer) = stream.into_split();
        let (conn, _tx, rx) = remoc::Connect::io::<tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf, ProcessMessage<T>, ProcessMessage<T>, remoc::codec::PostbagSlim>(
            remoc::Cfg::balanced(), 
            reader, 
            writer, 
        )
            .await
            .expect("Failed to create remoc connection");

        let cancel_token = CancellationToken::new();
        let c_token = cancel_token.clone();
        let fut = async move { 
            tokio::select! {
                _ = c_token.cancelled() => {}
                _ = conn => {
                }
            }
        };
        tokio::task::spawn(fut);

        let mut message_rx: Option<remoc::rch::mpsc::Receiver<ProcessMessage<T>>> = None;

        let mut rx = rx;
        while let Some(msg) = rx.recv().await.expect("Failed to receive message from parent process") {
            match msg {
                ProcessMessage::LoadMpsc { recv, recieved } => {
                    println!("Received LoadMpsc message");
                    message_rx = Some(recv);
                    recieved.send(()).expect("Failed to send LoadMpsc acknowledgement");
                    break;
                }
                _ => {
                    panic!("Received unexpected message before LoadMpsc");
                }
            }
        }

        let mut message_rx = message_rx.expect("Did not receive LoadMpsc message");

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Start up the run function as a task
        tokio::task::spawn_local(T::run(rx));

        loop {
            tokio::select! {
                msg = message_rx.recv() => {
                    let Ok(msg) = msg else {
                        panic!("Failed to receive message from parent process");
                    };
                    let Some(msg) = msg else {
                        continue;
                    };
                    match msg {
                        ProcessMessage::Message { data } => {
                            let _ = tx.send(data);
                            //tokio::task::yield_now().await;
                        }
                        ProcessMessage::LoadMpsc { .. } => {
                            panic!("Received unexpected LoadMpsc message");
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    break;
                }
            }
        }
    }

    /// Gets the state of the executor
    pub fn get_state(&self) -> &ConcurrentExecutorState<T> {
        match self {
            Self::Local { state, .. } => &state,
            Self::Threaded { state, .. } => &state,
            Self::Process { state, .. } => &state,
        }
    }

    /// Sends a message to the executor
    pub async fn send(&self, msg: T::Message) -> Result<(), BaseError> {
        match self {
            Self::Local { message_tx, .. } | Self::Threaded { message_tx, ..} => {
                message_tx.send(Message::Data { data: msg })?;
            }
            Self::Process { message_tx, .. } => {
                match message_tx.send(ProcessMessage::Message { data: Message::Data { data: msg }}).await {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(format!("Failed to send message to process executor: {}", e.to_string()).into());
                    }
                };
            }
        }
        Ok(())
    }

    /// Creates a new oneshot sender/receiver pair
    /// according to the type of executor
    /// 
    /// This should replace all use of tokio::sync::oneshot::channel function
    pub fn create_oneshot<U: Serialize + DeserializeOwned + Send + 'static>(&self) -> (OneshotSender<U>, OneshotReceiver<U>) {
        match self {
            Self::Local { .. } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (OneshotSender::new_local(tx), OneshotReceiver::new_local(rx))
            }
            Self::Threaded { .. } => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (OneshotSender::new_local(tx), OneshotReceiver::new_local(rx))
            }
            Self::Process { .. } => {
                let (tx, rx) = remoc::rch::oneshot::channel();
                (OneshotSender::new_process(tx), OneshotReceiver::new_process(rx))
            }
        }
    }

    /// Waits for the executor to finish executing(?)
    pub async fn wait(&self) -> Result<(), BaseError> {
        match self {
            Self::Local { .. } => {
                // Local executors run in the background, so we just wait for the cancel token
                self.get_state().cancel_token.cancelled().await;
            }
            Self::Threaded { .. } => {
                // Threaded executors run in the background, so we just wait for the cancel token
                self.get_state().cancel_token.cancelled().await;
            }
            Self::Process { proc_handle, .. } => {
                let proc_handle = proc_handle.clone();
                let cancel_token = self.get_state().cancel_token.clone();
                let fut = async move {
                    let mut r = proc_handle.write().await;
                    tokio::select! {
                        _ = cancel_token.cancelled() => {}
                        _ = r.wait() => {
                            // Cancel the token
                            cancel_token.cancel();
                        }
                    }
                };
                fut.await;
            }
        }
        Ok(())
    }

    /// Shuts down the executor
    pub fn shutdown(&self) -> Result<(), BaseError> {
        match self {
            Self::Local { message_tx, .. } => {
                message_tx.send(Message::Shutdown)?;
                self.get_state().cancel_token.cancel();
            }
            Self::Threaded { message_tx, .. } => {
                message_tx.send(Message::Shutdown)?;
                self.get_state().cancel_token.cancel();
            }
            Self::Process { message_tx, proc_handle, .. } => {
                let _ = message_tx.send(ProcessMessage::Message { data: Message::Shutdown });
                // Give some time for the process to exit gracefully
                let proc_handle = proc_handle.clone();
                self.get_state().cancel_token.cancel();
                let fut = async move {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    // If the process is still running, kill it
                    match proc_handle.write().await.kill().await {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("Failed to kill process: {}", e);
                        }
                    };    
                };
                tokio::task::spawn(fut);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CloneableConcurrentExecutor<T: ConcurrentlyExecute>(pub Arc<ConcurrentExecutor<T>>);
impl<T: ConcurrentlyExecute> std::ops::Deref for CloneableConcurrentExecutor<T> {
    type Target = ConcurrentExecutor<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::LocalOptions;

    #[test]
    // Dummy test
    fn test_spawn_in_local() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build_local(LocalOptions::default())
            .unwrap();

        runtime.block_on(async {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            tokio::task::spawn(async move {
                println!("Hello from task!");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                tokio::task::yield_now().await;
                let _ = tx.send(());
            });
            let _ = rx.recv().await;
        });
    }
}