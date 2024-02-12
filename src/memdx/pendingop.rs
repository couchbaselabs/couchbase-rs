use crate::memdx::client::Client;
use std::io;
use std::sync::mpsc::Sender;

pub trait PendingOp {
    fn cancel(&mut self, e: io::Error);
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub(crate) struct ClientPendingOp {
    opaque: u32,
    cancel_chan: Sender<u32>,
}

impl ClientPendingOp {
    pub fn new(opaque: u32, cancel_chan: Sender<u32>) -> Self {
        ClientPendingOp {
            opaque,
            cancel_chan,
        }
    }
}

impl PendingOp for ClientPendingOp {
    fn cancel(&mut self, e: io::Error) {
        self.cancel_chan.send(self.opaque);
    }
}
