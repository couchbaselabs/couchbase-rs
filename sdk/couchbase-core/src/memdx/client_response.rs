use crate::memdx::client::ResponseContext;
use crate::memdx::packet::ResponsePacket;

#[derive(Debug)]
pub struct ClientResponse {
    packet: ResponsePacket,
    response_context: ResponseContext,
}

impl ClientResponse {
    pub fn new(packet: ResponsePacket, response_context: ResponseContext) -> Self {
        Self {
            packet,
            response_context,
        }
    }

    pub fn packet(&self) -> &ResponsePacket {
        &self.packet
    }

    pub fn response_context(&self) -> &ResponseContext {
        &self.response_context
    }
}

impl Drop for ClientResponse {
    fn drop(&mut self) {}
}
