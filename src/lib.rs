pub(crate) mod ipcmux;
pub mod channel;

use std::{sync::Arc, time::Duration};

use ipc_channel::ipc::{IpcOneShotServer, IpcSender};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{runtime::{LocalOptions}, sync::{OwnedSemaphorePermit, Semaphore}};
use tokio_util::sync::CancellationToken;

use crate::{channel::ServerContext, ipcmux::IpcMessage};
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
    pub start_timeout: Duration,
    pub debug_print: bool,
}

impl ProcessOpts {
    pub fn new(cmd_argv: Vec<String>) -> Self {
        Self {
            cmd_argv,
            cmd_envs: vec![],
            start_timeout: Duration::from_secs(10),
            debug_print: false,
        }
    }

    pub fn with_envs(mut self, envs: Vec<(String, String)>) -> Self {
        self.cmd_envs = envs;
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

#[allow(async_fn_in_trait)]
/// Trait for types that can be executed concurrently
pub trait ConcurrentlyExecute: Send + Sync + 'static {
    type BootstrapData: Serialize + for<'a> Deserialize<'a> + 'static;

    async fn run(
        bootstrap_data: Self::BootstrapData,
        client_ctx: ClientContext
    );
}

/// State for a concurrent executor
pub struct ConcurrentExecutorState<T: ConcurrentlyExecute> {
    pub cancel_token: CancellationToken,
    pub sema: Arc<Semaphore>,
    _marker: std::marker::PhantomData<T>,
}

// Needed as C
impl<T: ConcurrentlyExecute> Clone for ConcurrentExecutorState<T> {
    fn clone(&self) -> Self {
        Self {
            cancel_token: self.cancel_token.clone(),
            sema: self.sema.clone(),
            _marker: std::marker::PhantomData,
        }
    }
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
    shutdown_chan: tokio::sync::mpsc::UnboundedSender<()>,
    permit: OwnedSemaphorePermit,

    // Sends IpcMessage to the child via ipcmux
    //
    // The server will also spawn a task to listen for messages from the child
    // which is a IpcReceiver<IpcMessage>
    mux: ipcmux::IpcMux,
    server_context: ServerContext,

    child_pid: u32,
}

/// Concurrent tokio execution
///
/// Assumes use of local tokio runtime
pub struct ConcurrentExecutor<T: ConcurrentlyExecute> {
    inner: ConcurrentExecutorInner<T>,
}

impl<T: ConcurrentlyExecute> Drop for ConcurrentExecutor<T> {
    fn drop(&mut self) {
        let _ = self.shutdown_in_task();
    }
}

impl<T: ConcurrentlyExecute> ConcurrentExecutor<T> {
    /// Creates a new process concurrent executor
    /// that runs in a separate process
    pub async fn new<Return: Send + 'static>(
        cs_state: ConcurrentExecutorState<T>,
        mut opts: ProcessOpts,
        initial_data: impl FnOnce(&Self) -> (T::BootstrapData, Return) + Send + 'static,
    ) -> Result<(Self, Return), BaseError> {
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
        let mut proc_handle = cmd.spawn()?;

        let child_pid = proc_handle.id().unwrap_or(0);
        
        // Wait for a connection
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
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
                let ntx: IpcSender<IpcMessage> = ipc_channel::ipc::IpcSender::connect(msg.name).unwrap();
                
                let cei = ConcurrentExecutor {
                    inner: ConcurrentExecutorInner {
                        state: cs_state.clone(),
                        shutdown_chan: shutdown_tx,
                        permit,
                        mux: ipcmux::IpcMux::new(), // Dummy mux for now
                        server_context: ServerContext {
                            chan: ntx.clone(),
                        },
                        child_pid
                    }
                };
                let (initial_data, ret) = initial_data(&cei);
                let msg = ipcmux::IpcMessage::new(ipcmux::IPC_BOOTSTRAP_DATA_ID, false, &initial_data).unwrap();
                ntx.send(msg).unwrap(); // Send initial data to establish the connection
                
                if debug_print {
                    println!("Connected to child process");
                }

                // Send the IpcSender back to the main
                let _ = tx.send((rx, cei, ret));
            });
        });

        let (rx, cei, ret) = match tokio::time::timeout(opts.start_timeout, rx).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(format!("Failed to receive IPC connection from child process: {}", e.to_string()).into()),
            Err(_) => return Err("Timed out waiting for IPC connection from child process".into()),
        };

        let ipcmux_ref = cei.inner.mux.clone();
        let ctoken = cei.inner.state.cancel_token.clone();
        tokio::task::spawn(async move {
            ipcmux_ref.run(rx.to_stream(), ctoken).await;
        });

        let ctoken = cei.inner.state.cancel_token.clone();
        tokio::task::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        // Wait for the process to exit
                        let _ = proc_handle.kill().await;
                        ctoken.cancel();
                        break;
                    }
                    _ = proc_handle.wait() => {
                        // Process exited, cancel the token
                        ctoken.cancel();
                        break;
                    }
                }
            }
        });

        Ok((cei,ret))
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
        let (rx, server_conn_pipe) = ipc_channel::ipc::IpcOneShotServer::<IpcMessage>::new().expect("Failed to create IPC oneshot server");
        // Send the server the IpcSender to connect to
        let bootstrap = ipcmux::IpcMuxBootstrap { name: server_conn_pipe };
        let msg = IpcMessage::new(ipcmux::IPC_BOOTSTRAP_ID, false, &bootstrap).expect("Failed to create IpcMessage for IpcMuxBootstrap");
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
        assert!(ini_msg.id == ipcmux::IPC_BOOTSTRAP_DATA_ID, "Initial message from server must be BootstrapData");
        let initial_data = ini_msg.deserialize::<T::BootstrapData>().expect("Failed to deserialize BootstrapData from server");
        if debug_print {
            println!("Received initial bootstrap data from server");
        }

        let mux = ipcmux::IpcMux::new();
        let ipcmux_ref = mux.clone();
        tokio::task::spawn(async move {
            if debug_print {
                println!("Starting message muxing task");
            }

            ipcmux_ref.run(rx.to_stream(), c_token).await;
        });

        // Start up the run function as a task
        if debug_print {
            println!("Starting run function");
        }

        T::run(initial_data, ClientContext { chan: tx_ipcmux, _mux: mux, cancel_token: cancel_token.clone() }).await;
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

    /// Creates a new oneshot sender/receiver pair
    /// according to the type of executor
    /// 
    /// This should replace all use of tokio::sync::oneshot::channel function
    pub fn create_oneshot<U: Serialize + DeserializeOwned + Send + 'static>(&self) -> (channel::OneshotSender<U>, channel::OneshotReceiver<U>) {
        let (tx, rx) = channel::channel(self.inner.mux.clone(), self.inner.state.cancel_token.clone());
        (tx, rx)
    }    
    
    /// Creates a new oneshot sender/receiver pair
    /// according to the type of executor
    /// 
    /// This should replace all use of tokio::sync::oneshot::channel function
    pub fn create_multi<U: Serialize + DeserializeOwned + Send + 'static>(&self) -> (channel::MultiSender<U>, channel::MultiReceiver<U>) {
        let (tx, rx) = channel::multi_channel(self.inner.mux.clone(), self.inner.state.cancel_token.clone());
        (tx, rx)
    }

    /// Returns the server context
    /// 
    /// This can be used with OneShotSender::server or MultiSender::server
    pub fn server_context(&self) -> &ServerContext {
        &self.inner.server_context
    }

    /// Waits for the executor to finish executing
    pub async fn wait(&self) -> Result<(), BaseError> {
        self.get_state().cancel_token.cancelled().await;
        Ok(())
    }

    /// Shuts down the executor
    pub async fn shutdown(&self) -> Result<(), BaseError> {
        // Send message to the task to shut down
        let _ = self.inner.shutdown_chan.send(());
        // Wait for the task to finish
        self.get_state().cancel_token.cancelled().await;
        Ok(())
    }

    pub fn shutdown_in_task(&self) -> Result<(), BaseError> {
        // Send message to the task to shut down
        let _ = self.inner.shutdown_chan.send(());
        Ok(())
    }

    /// Returns the PID of the created child
    pub fn child_pid(&self) -> u32 {
        self.inner.child_pid
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