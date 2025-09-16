pub(crate) mod ipcmux;
pub mod channel;

use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use ipc_channel::ipc::{IpcOneShotServer, IpcSender};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{runtime::{LocalOptions}, sync::{mpsc::UnboundedReceiver, OwnedSemaphorePermit, RwLock, Semaphore}};
use tokio_util::sync::CancellationToken;
use tokio::process::Child;

use crate::ipcmux::IpcMessage;
pub use crate::channel::{ClientContext, OneshotSender, OneshotReceiver, MultiSender, MultiReceiver};

pub type BaseError = Box<dyn std::error::Error + Send + Sync>;

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
    pub start_timeout: Duration,
    pub debug_print: bool,
}

impl ProcessOpts {
    pub fn new(cmd_argv: Vec<String>) -> Self {
        Self {
            cmd_argv,
            cmd_envs: vec![],
            mem_soft_limit: 0,
            mem_hard_limit: 0,
            start_timeout: Duration::from_secs(10),
            debug_print: false,
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

    pub fn with_debug_print(mut self, debug: bool) -> Self {
        self.debug_print = debug;
        self
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
enum ProcessMessage<T: ConcurrentlyExecute> {
    Message {
        data: Message<T>,
    },
    BootstrapData {
        data: T::BootstrapData,
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
    type BootstrapData: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static;

    async fn run(
        rx: UnboundedReceiver<Message<Self>>, 
        bootstrap_data: Self::BootstrapData,
        client_ctx: ClientContext
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

/// Internal enum to store the server state of the executor
struct ConcurrentExecutorInner<T: ConcurrentlyExecute> {
    state: ConcurrentExecutorState<T>,
    proc_handle: Arc<RwLock<Child>>,
    permit: OwnedSemaphorePermit,

    // Sends IpcMessage to the child via ipcmux
    //
    // The server will also spawn a task to listen for messages from the child
    // which is a IpcReceiver<IpcMessage>
    mux: ipcmux::IpcMux,
    tx: IpcSender<ProcessMessage<T>>, // Option to enable sending an initial None message
}

/// Concurrent tokio execution
///
/// Assumes use of local tokio runtime
pub struct ConcurrentExecutor<T: ConcurrentlyExecute> {
    inner: ConcurrentExecutorInner<T>,
}

impl<T: ConcurrentlyExecute> Drop for ConcurrentExecutor<T> {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl<T: ConcurrentlyExecute> ConcurrentExecutor<T> {
    /// Creates a new process concurrent executor
    /// that runs in a separate process
    pub async fn new(
        cs_state: ConcurrentExecutorState<T>,
        mut opts: ProcessOpts,
        initial_data: impl FnOnce(&Self) -> T::BootstrapData + Send + 'static,
    ) -> Result<Self, BaseError> {
        let debug_print = opts.debug_print;
        let permit = cs_state.sema.clone().acquire_owned().await?;

        let (ipc_channel, name) = IpcOneShotServer::<IpcMessage>::new().map_err(|e| format!("Failed to create IPC oneshot server: {}", e.to_string()))?;

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
                "CONCURRENT_EXECUTOR_SERVER_NAME",
                name.as_str(),
            ),
            (
                "CONCURRENT_EXECUTOR_DEBUG_PRINT",
                if debug_print { "1" } else { "0" },
            )
        ];
        for (k, v) in opts.cmd_envs.iter() {
            env.push((k.as_str(), v.as_str()));
        }
        let mut cmd = tokio::process::Command::new(exe);
        cmd.args(args);
        cmd.envs(env);
        cmd.kill_on_drop(true);
        let proc_handle = cmd.spawn()?;

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
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build_local(LocalOptions::default())
                .expect("Failed to create tokio runtime");

            rt.block_on(async move {
                // Connect to the client, this creates a unidirectional channel between the server and client
                let (rx, client_conn_pipe) = ipc_channel.accept().unwrap();
                
                if debug_print {
                    println!("Accepted connection from child process");
                    println!("Message from child process: {:?}", client_conn_pipe);
                }
                
                // Deserialize the IpcSender from the message
                let msg: ipcmux::IpcMuxBootstrap = client_conn_pipe.deserialize().unwrap();
                let ntx: IpcSender<ProcessMessage<T>> = ipc_channel::ipc::IpcSender::connect(msg.name).unwrap();
                
                let cei = ConcurrentExecutor {
                    inner: ConcurrentExecutorInner {
                        state: cs_state.clone(),
                        proc_handle: Arc::new(RwLock::new(proc_handle)),
                        permit,
                        mux: ipcmux::IpcMux::new(), // Dummy mux for now
                        tx: ntx.clone()
                    }
                };
                let initial_data = initial_data(&cei);
                ntx.send(ProcessMessage::BootstrapData { data: initial_data }).unwrap(); // Send an initial None message to establish the connection
                
                if debug_print {
                    println!("Connected to child process");
                }

                // Send the IpcSender back to the main
                let _ = tx.send((rx, cei));
            });
        });

        let (rx, cei) = match tokio::time::timeout(opts.start_timeout, rx).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(format!("Failed to receive IPC connection from child process: {}", e.to_string()).into()),
            Err(_) => return Err("Timed out waiting for IPC connection from child process".into()),
        };

        let ipcmux_ref = cei.inner.mux.clone();
        let ctoken = cei.inner.state.cancel_token.clone();
        tokio::task::spawn(async move {
            ipcmux_ref.run(rx.to_stream(), ctoken).await;
        });

        Ok(cei)
    }

    /// Starts up a process client
    /// The process should have first setup a tokio localruntime first
    pub async fn run_process_client() {
        let debug_print = std::env::var("CONCURRENT_EXECUTOR_DEBUG_PRINT").unwrap_or_else(|_| "0".to_string()) == "1";
        if debug_print {
            println!("Starting process client");
        }

        let name = std::env::var("CONCURRENT_EXECUTOR_SERVER_NAME").expect("CONCURRENT_EXECUTOR_SERVER_NAME not set");
        // Connect to the server
        let tx_ipcmux = ipc_channel::ipc::IpcSender::<IpcMessage>::connect(name).expect("Failed to connect to IPC server");
        
        if debug_print {
            println!("Connected to server, setting up ipcmux");
        }

        if debug_print {
            println!("Set IPC_TX");
        }

        // Create a second pipe for the server to send messages to the client
        let (rx, server_conn_pipe) = ipc_channel::ipc::IpcOneShotServer::<ProcessMessage<T>>::new().expect("Failed to create IPC oneshot server");
        // Send the server the IpcSender to connect to
        let bootstrap = ipcmux::IpcMuxBootstrap { name: server_conn_pipe };
        let msg = IpcMessage {
            id: ipcmux::IPC_BOOTSTRAP_ID,
            mpsc: false,
            data: serde_bytes::ByteBuf::from(bincode::serde::encode_to_vec(bootstrap, bincode::config::standard()).expect("Failed to serialize IpcMuxBootstrap")),
        };
        tx_ipcmux.send(msg).expect("Failed to send IpcMuxBootstrap to server");
        
        if debug_print {
            println!("Sent bootstrap to server, waiting for connection");
        }

        let (rx, ini_msg) = rx.accept().expect("Failed to accept IPC connection from server");
        
        if debug_print {
            println!("Connected to server");
        }

        let cancel_token = CancellationToken::new();
        let c_token = cancel_token.clone();
        let (msgtx, msgrx) = tokio::sync::mpsc::unbounded_channel::<Message<T>>();
        let initial_data = match ini_msg {
            ProcessMessage::BootstrapData { data } => data,
            _ => panic!("Initial message from server must be BootstrapData"),
        };
        let fut = async move { 
            if debug_print {
                println!("Starting message forwarding task");
            }

            let mut rx = rx.to_stream();
            loop {
                tokio::select! {
                    _ = c_token.cancelled() => {
                        if debug_print {
                            println!("Cancellation token triggered, exiting message forwarding task");
                        }
                        break;
                    }
                    s = rx.next() => {
                        if debug_print {
                            println!("Received message from server pipe");
                        }

                        let Some(Ok(msg)) = s else {
                            continue;
                        };

                        let msg = match msg {
                            ProcessMessage::Message { data } => data,
                            ProcessMessage::BootstrapData { .. } => {
                                if debug_print {
                                    println!("Received unexpected BootstrapData message from server, ignoring");
                                }
                                continue;
                            }
                        };

                        match msg {
                            Message::Data { data } => {
                                if debug_print {
                                    println!("Received message from server");
                                }

                                let _ = msgtx.send(Message::Data { data });
                            },
                            Message::Shutdown => {
                                break;
                            }
                        }
                    }
                }
            }
        };
        tokio::task::spawn(fut);

        // Start up the run function as a task
        if debug_print {
            println!("Starting run function");
        }

        T::run(msgrx, initial_data, ClientContext { chan: tx_ipcmux }).await;
        cancel_token.cancel();
    }

    /// Gets the state of the executor
    pub fn get_state(&self) -> &ConcurrentExecutorState<T> {
        &self.inner.state
    }

    /// Returns the owned permit
    pub fn get_permit(&self) -> &OwnedSemaphorePermit {
        &self.inner.permit
    }

    /// Returns the owned permit as a mutable reference
    pub fn get_permit_mut(&mut self) -> &mut OwnedSemaphorePermit {
        &mut self.inner.permit
    }

    /// Sends a message to the executor
    pub fn send_raw(&self, msg: Message<T>) -> Result<(), BaseError> {
        match self.inner.tx.send(ProcessMessage::Message { data: msg }) {
            Ok(_) => {},
            Err(e) => {
                return Err(format!("Failed to send message to process executor: {}", e.to_string()).into());
            }
        };

        Ok(())
    }

    /// Sends a message to the executor
    pub fn send(&self, msg: T::Message) -> Result<(), BaseError> {
        self.send_raw( Message::Data { data: msg } )
    }

    /// Creates a new oneshot sender/receiver pair
    /// according to the type of executor
    /// 
    /// This should replace all use of tokio::sync::oneshot::channel function
    pub fn create_oneshot<U: Serialize + DeserializeOwned + Send + 'static>(&self) -> (channel::OneshotSender<U>, channel::OneshotReceiver<U>) {
        let (tx, rx) = channel::channel(self.inner.mux.clone());
        (tx, rx)
    }    
    
    /// Creates a new oneshot sender/receiver pair
    /// according to the type of executor
    /// 
    /// This should replace all use of tokio::sync::oneshot::channel function
    pub fn create_multi<U: Serialize + DeserializeOwned + Send + 'static>(&self) -> (channel::MultiSender<U>, channel::MultiReceiver<U>) {
        let (tx, rx) = channel::multi_channel(self.inner.mux.clone());
        (tx, rx)
    }

    /// Waits for the executor to finish executing
    pub async fn wait(&self) -> Result<(), BaseError> {
        let proc_handle = self.inner.proc_handle.clone();
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
        Ok(())
    }

    /// Shuts down the executor
    pub fn shutdown(&self) -> Result<(), BaseError> {
        // Give some time for the process to exit gracefully
        let proc_handle = self.inner.proc_handle.clone();
        self.get_state().cancel_token.cancel();
        let fut = async move {
            match proc_handle.write().await.kill().await {
                Ok(_) => {},
                Err(e) => {
                    eprintln!("Failed to kill process: {}", e);
                }
            };    
        };
        tokio::task::spawn(fut);
        Ok(())
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