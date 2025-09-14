use tokio::sync::oneshot::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use std::{collections::HashMap, sync::{Mutex, OnceLock}};
use serde::{Serialize, Deserialize};
use futures_util::stream::StreamExt;
use std::sync::{Arc, RwLock};

use crate::BaseError;

pub const IPC_BOOTSTRAP_ID: u64 = 1292;

#[derive(Default)]
struct IpcMuxMap {
    map: HashMap<u64, Sender<IpcMessage>>,
    last: u64,
}

/// Multiplexing multiple IPC channels over a single connection
#[derive(Clone)]
pub(crate) struct IpcMux {
    cmap: Arc<RwLock<IpcMuxMap>>,
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
        let mut rg = self.cmap.write().unwrap();
        let id = {
            let mut last_id = rg.last;
            loop {
                last_id = last_id.wrapping_add(1);
                if !IpcMessage::is_reserved(last_id) && !rg.map.contains_key(&last_id) {
                    break last_id;
                }
            }
        };
        rg.last = id;
        let (tx, rx) = tokio::sync::oneshot::channel();
        rg.map.insert(id, tx);
        (id, rx)
    }

    pub fn unregister(&self, id: &u64) {
        self.cmap.write().unwrap().map.remove(id);
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
                            if let Some(tx) = self.cmap.write().unwrap().map.remove(&data.id) {
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
    rx: Option<Receiver<IpcMessage>>,
    id: u64,
    mux: IpcMux,
    _marker: std::marker::PhantomData<T>,
}

impl<T: for<'de> Deserialize<'de>> OneshotReceiver<T> {
    /// Receives a message from the oneshot channel
    pub async fn recv(mut self) -> Result<T, BaseError> {
        let rx = std::mem::take(&mut self.rx);
        let Some(rx) = rx else {
            return Err("OneshotReceiver already consumed".into());
        };
        let resp = rx.await?;
        resp.deserialize()
    }
}

impl<T> Drop for OneshotReceiver::<T> {
    fn drop(&mut self) {
        self.mux.unregister(&self.id);
    }
}

pub(crate) fn channel<T: for<'de> Deserialize<'de> + Serialize>(mux: IpcMux) -> (OneshotSender<T>, OneshotReceiver<T>) {
    let (id, rx) = mux.register();
    let tx = OneshotSender {
        id,
        _marker: std::marker::PhantomData,
    };
    let rx = OneshotReceiver {
        id,
        rx: Some(rx),
        mux,
        _marker: std::marker::PhantomData,
    };
    (tx, rx)
}

#[derive(Serialize, Deserialize)]
pub struct IpcMuxBootstrap {
    pub name: String,
}