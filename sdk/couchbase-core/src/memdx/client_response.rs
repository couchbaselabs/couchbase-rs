use crate::memdx::client::ResponseContext;
use crate::memdx::packet::ResponsePacket;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct ClientResponse {
    packet: ResponsePacket,
    has_more_sender: oneshot::Sender<bool>,

    response_context: ResponseContext,
}

impl ClientResponse {
    pub fn new(
        packet: ResponsePacket,
        has_more_sender: oneshot::Sender<bool>,
        response_context: ResponseContext,
    ) -> Self {
        Self {
            packet,
            has_more_sender,
            response_context,
        }
    }

    pub fn packet(&self) -> &ResponsePacket {
        &self.packet
    }

    pub fn send_has_more(self) {
        match self.has_more_sender.send(true) {
            Ok(_) => {}
            Err(_e) => {}
        };
    }

    pub fn response_context(&self) -> &ResponseContext {
        &self.response_context
    }
}
