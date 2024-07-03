use std::net::SocketAddr;

use tokio::sync::oneshot;

use crate::memdx::packet::ResponsePacket;

#[derive(Debug)]
pub(crate) struct ClientResponse {
    packet: ResponsePacket,
    has_more_sender: oneshot::Sender<bool>,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl ClientResponse {
    pub fn new(
        packet: ResponsePacket,
        has_more_sender: oneshot::Sender<bool>,
        local_addr: Option<SocketAddr>,
        peer_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            packet,
            has_more_sender,
            local_addr,
            peer_addr,
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
}
