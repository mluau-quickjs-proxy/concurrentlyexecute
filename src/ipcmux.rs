use tokio::sync::oneshot::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use std::{collections::HashMap, sync::{Mutex, OnceLock}};
use serde::{Serialize, Deserialize};
use futures_util::stream::StreamExt;
use std::sync::{Arc, RwLock};

use crate::BaseError;

pub const IPC_BOOTSTRAP_ID: u64 = 1292;

/// Multiplexing multiple IPC channels over a single connection
#[derive(Clone)]
pub struct IpcMux {
    cmap: Arc<RwLock<HashMap<u64, Sender<IpcMessage>>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IpcMessage {
    pub id: u64,
    pub data: serde_bytes::ByteBuf,
}

impl IpcMessage {
    pub fn deserialize<T: for<'de> Deserialize<'de>>(&self) -> Result<T, BaseError> {
        let (data, _) = bincode::serde::decode_from_slice(&self.data, bincode::config::standard())?;
        Ok(data)
    }

    pub const fn is_reserved(id: u64) -> bool {
        id == 0 || id == IPC_BOOTSTRAP_ID
    }
}

impl IpcMux {
    pub fn new() -> Self {
        Self {
            cmap: Arc::default(),
        }
    }

    pub fn register(&self) -> (u64, Receiver<IpcMessage>) {
        let rg = self.cmap.read().unwrap();
        let id = {
            loop {
                let id = rand::random::<u64>();
                if IpcMessage::is_reserved(id) {
                    continue; // reserved
                }
                if !rg.contains_key(&id) {
                    break id;
                }
            }
        };
        drop(rg);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmap.write().unwrap().insert(id, tx);
        (id, rx)
    }

    pub fn unregister(&mut self, id: &u64) {
        self.cmap.write().unwrap().remove(id);
    }

    pub async fn run(&self, mut rx: ipc_channel::asynch::IpcStream<IpcMessage>, cancel_token: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                Some(msg) = rx.next() => {
                    match msg {
                        Ok(data) => {
                            if IpcMessage::is_reserved(data.id) {
                                continue; // reserved
                            }
                            if let Some(tx) = self.cmap.write().unwrap().remove(&data.id) {
                                let _ = tx.send(data);
                            } else {
                                // Unknown id, ignore
                            }
                        }
                        Err(_) => {
                            // Connection closed
                            break;
                        }
                    }
                }
            }
        }
    }
}

pub static IPC_TX: OnceLock<Mutex<ipc_channel::ipc::IpcSender<IpcMessage>>> = OnceLock::new();

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct OneshotSender<T> {
    pub id: u64,
    pub _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> OneshotSender<T> {
    /// Sends a message to the given oneshot channel
    /// 
    /// Note: this must be run on the client process
    pub fn send(self, data: T) -> Result<(), BaseError> {
        let tx = IPC_TX.get().ok_or_else(|| "IPC_TX not initialized")?.lock()
        .map_err(|e| format!("Failed to lock IPC_TX: {}", e.to_string()))?;
        let msg = IpcMessage {
            id: self.id,
            data: serde_bytes::ByteBuf::from(bincode::serde::encode_to_vec(data, bincode::config::standard())?),
        };
        Ok(tx.send(msg)?)
    }
}

pub struct OneshotReceiver<T> {
    pub rx: Receiver<IpcMessage>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: for<'de> Deserialize<'de>> OneshotReceiver<T> {
    /// Receives a message from the oneshot channel
    pub async fn recv(self) -> Result<T, BaseError> {
        let resp = self.rx.await?;
        resp.deserialize()
    }
}

pub fn channel<T: for<'de> Deserialize<'de> + Serialize>(mux: &IpcMux) -> (OneshotSender<T>, OneshotReceiver<T>) {
    let (id, rx) = mux.register();
    let tx = OneshotSender {
        id,
        _marker: std::marker::PhantomData,
    };
    let rx = OneshotReceiver {
        rx,
        _marker: std::marker::PhantomData,
    };
    (tx, rx)
}

#[derive(Serialize, Deserialize)]
pub struct IpcMuxBootstrap {
    pub name: String,
}