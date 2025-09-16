use tokio::sync::{mpsc::UnboundedReceiver, oneshot::{Receiver}};
use crate::{ipcmux::{IpcMessage, IpcMux}, BaseError};
use serde::{Serialize, Deserialize};


#[derive(Clone)]
pub struct ClientContext {
    pub(crate) chan: ipc_channel::ipc::IpcSender<IpcMessage>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct OneshotSender<T> {
    id: u64,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize> OneshotSender<T> {
    /// Upgrades a oneshot sender to a ClientOneShotSender which can be used from a client process
    pub fn client(self, ctx: &ClientContext) -> ClientOneShotSender<T> {
        let chan = ctx.chan.clone();
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
    pub fn client(&self, ctx: &ClientContext) -> ClientMultiSender<T> {
        let chan = ctx.chan.clone();
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