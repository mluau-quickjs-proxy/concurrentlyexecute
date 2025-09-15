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

#[derive(Clone)]
pub struct ClientContext {
    pub(crate) chan: Option<ipc_channel::ipc::IpcSender<IpcMessage>>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct OneshotSender<T> {
    id: u64,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> OneshotSender<T> {
    /// Upgrades a oneshot sender to a ClientOneShotSender which can be used from a client process
    /// 
    /// Panics if not run from a child process
    pub fn upgrade(self, ctx: &ClientContext) -> ClientOneShotSender<T> {
        let chan = ctx.chan.clone().expect("Upgrade must be called on a child process");
        ClientOneShotSender {
            id: self.id,
            chan,
            _marker: self._marker

        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct MultiSender<T> {
    id: u64,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> MultiSender<T> {
    /// Upgrades a sender to a ClientMulti which can be used from a client process
    /// 
    /// Panics if not run from a child process
    pub fn upgrade(&self, ctx: &ClientContext) -> ClientMultiSender<T> {
        let chan = ctx.chan.clone().expect("Upgrade must be called on a child process");
        ClientMultiSender {
            id: self.id,
            chan,
            _marker: self._marker

        }
    }
}

/// A oneshot sender can only send messages from a client process
/// 
/// In order to ensure this (and get the send connection from the client),
/// this struct acts as a intermediary to allow sending
pub struct ClientOneShotSender<T> {
    id: u64,
    chan: ipc_channel::ipc::IpcSender<IpcMessage>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> ClientOneShotSender<T> {
    /// Sends a message to the given oneshot channel
    pub fn send(self, data: T) -> Result<(), BaseError> {
        let msg = IpcMessage {
            id: self.id,
            mpsc: false,
            data: serde_bytes::ByteBuf::from(bincode::serde::encode_to_vec(data, bincode::config::standard())?),
        };
        Ok(self.chan.send(msg)?)
    }
}

/// A multi sender can only send messages from a client process
/// 
/// In order to ensure this (and get the send connection from the client),
/// this struct acts as a intermediary to allow sending
pub struct ClientMultiSender<T> {
    id: u64,
    chan: ipc_channel::ipc::IpcSender<IpcMessage>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> ClientMultiSender<T> {
    /// Sends a message to the given oneshot channel
    pub fn send(&self, data: T) -> Result<(), BaseError> {
        let msg = IpcMessage {
            id: self.id,
            mpsc: true,
            data: serde_bytes::ByteBuf::from(bincode::serde::encode_to_vec(data, bincode::config::standard())?),
        };
        Ok(self.chan.send(msg)?)
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

pub struct MultiReceiver<T> {
    rx: UnboundedReceiver<IpcMessage>,
    id: u64,
    mux: IpcMux,
    _marker: std::marker::PhantomData<T>,
}

impl<T: for<'de> Deserialize<'de>> MultiReceiver<T> {
    /// Receives a message from the multi channel
    pub async fn recv(&mut self) -> Result<T, BaseError> {
        let Some(resp) = self.rx.recv().await else {
            return Err("MultiReceiver channel closed".into());
        };
        resp.deserialize()
    }
}

impl<T> Drop for MultiReceiver::<T> {
    fn drop(&mut self) {
        self.mux.unregister_mpsc(&self.id);
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

pub(crate) fn multi_channel<T: for<'de> Deserialize<'de> + Serialize>(mux: IpcMux) -> (MultiSender<T>, MultiReceiver<T>) {
    let (id, rx) = mux.register_mpsc();
    let tx = MultiSender {
        id,
        _marker: std::marker::PhantomData,
    };
    let rx = MultiReceiver {
        id,
        rx,
        mux,
        _marker: std::marker::PhantomData,
    };
    (tx, rx)
}

#[derive(Serialize, Deserialize)]
pub struct IpcMuxBootstrap {
    pub name: String,
}