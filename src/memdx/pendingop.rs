use crate::memdx::client::CancellationSender;
use crate::memdx::error::CancellationErrorKind;
use log::debug;

pub trait PendingOp {
    fn cancel(&mut self, e: CancellationErrorKind);
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub(crate) struct ClientPendingOp {
    opaque: u32,
    cancel_chan: CancellationSender,
}

impl ClientPendingOp {
    pub fn new(opaque: u32, cancel_chan: CancellationSender) -> Self {
        ClientPendingOp {
            opaque,
            cancel_chan,
        }
    }
}

impl PendingOp for ClientPendingOp {
    fn cancel(&mut self, e: CancellationErrorKind) {
        match self.cancel_chan.send((self.opaque, e)) {
            Ok(_) => {}
            Err(e) => {
                debug!("Failed to send cancel to channel {}", e);
            }
        };
    }
}
