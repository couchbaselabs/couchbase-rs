use crate::memdx::client::ResponseContext;
use crate::memdx::packet::ResponsePacket;
use std::sync::Arc;

#[derive(Debug)]
pub struct ClientResponse {
    packet: ResponsePacket,
    response_context: Arc<ResponseContext>,
}

impl ClientResponse {
    pub fn new(packet: ResponsePacket, response_context: Arc<ResponseContext>) -> Self {
        Self {
            packet,
            response_context,
        }
    }

    pub fn packet(self) -> ResponsePacket {
        self.packet
    }

    pub fn response_context(&self) -> Arc<ResponseContext> {
        self.response_context.clone()
    }
}
