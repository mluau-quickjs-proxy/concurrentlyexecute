use tokio::sync::{mpsc::{UnboundedReceiver, UnboundedSender}, oneshot::{Receiver, Sender}};
use tokio_util::sync::CancellationToken;
use std::{collections::HashMap, sync::RwLock};
use serde::{Serialize, Deserialize};
use futures_util::stream::StreamExt;
use std::sync::Arc;

use crate::BaseError;

pub const IPC_BOOTSTRAP_ID: u64 = 1292;

#[derive(Default)]
struct IpcMuxMap {
    map: HashMap<u64, Sender<IpcMessage>>,
    last: u64,
}

#[derive(Default)]
struct IpcMuxMapMpsc {
    map: HashMap<u64, UnboundedSender<IpcMessage>>,
    last: u64,
}

/// Multiplexing multiple IPC channels over a single connection
#[derive(Clone)]
pub(crate) struct IpcMux {
    cmap: Arc<RwLock<IpcMuxMap>>,
    mpsc_cmap: Arc<RwLock<IpcMuxMapMpsc>>
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct IpcMessage {
    pub id: u64,
    pub mpsc: bool,
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
            mpsc_cmap: Arc::default(),
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

    pub fn register_mpsc(&self) -> (u64, UnboundedReceiver<IpcMessage>) {
        let mut rg = self.mpsc_cmap.write().unwrap();
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        rg.map.insert(id, tx);
        (id, rx)
    }

    pub fn unregister(&self, id: &u64) {
        self.cmap.write().unwrap().map.remove(id);
    }

    pub fn unregister_mpsc(&self, id: &u64) {
        self.mpsc_cmap.write().unwrap().map.remove(id);
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

                            if data.mpsc {
                                if let Some(tx) = self.mpsc_cmap.read().unwrap().map.get(&data.id) {
                                    let _ = tx.send(data);
                                }
                                continue;
                            }

                            if let Some(tx) = self.cmap.write().unwrap().map.remove(&data.id) {
                                let _ = tx.send(data);
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

#[derive(Serialize, Deserialize)]
pub struct IpcMuxBootstrap {
    pub name: String,
}