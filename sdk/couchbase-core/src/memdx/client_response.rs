use std::net::SocketAddr;

use crate::memdx::client::ResponseContext;
use crate::memdx::packet::ResponsePacket;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct ClientResponse {
    packet: ResponsePacket,
    has_more_sender: oneshot::Sender<bool>,

    response_context: ResponseContext,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl ClientResponse {
    pub fn new(
        packet: ResponsePacket,
        has_more_sender: oneshot::Sender<bool>,
        local_addr: Option<SocketAddr>,
        peer_addr: Option<SocketAddr>,
        response_context: ResponseContext,
    ) -> Self {
        Self {
            packet,
            has_more_sender,
            local_addr,
            peer_addr,
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

    // local_addr is the address corresponding to the server, i.e. local to the responder.
    pub fn local_addr(&self) -> &Option<SocketAddr> {
        &self.local_addr
    }

    // peer_addr is the address corresponding to the client, i.e. local to the recipient.
    pub fn peer_addr(&self) -> &Option<SocketAddr> {
        &self.peer_addr
    }

    pub fn response_context(&self) -> &ResponseContext {
        &self.response_context
    }
}
