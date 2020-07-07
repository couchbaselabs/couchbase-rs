mod codec;
mod protocol;

use bytes::{Bytes, BytesMut};
use codec::KeyValueCodec;
use futures::pin_mut;
use futures::sink::SinkExt;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct KvEndpoint {
    remote_addr: SocketAddr,
}

impl KvEndpoint {
    pub fn new(hostname: String, port: usize) -> Self {
        let remote_addr = format!("{}:{}", hostname, port).parse().unwrap();
        Self { remote_addr }
    }

    pub async fn connect(&self) {
        let mut socket = TcpStream::connect(self.remote_addr).await.unwrap();
        println!("Connected to socket {:?}", socket);
        let (r, w) = socket.split();
        let mut output = FramedWrite::new(w, KeyValueCodec::new());
        let mut input = FramedRead::new(r, KeyValueCodec::new());

        pin_mut!(input);

        self.negotiate_hello(input, output).await;
    }

    async fn negotiate_hello(
        &self,
        mut input: Pin<&mut dyn Stream<Item = Result<BytesMut, std::io::Error>>>,
        mut output: impl Sink<Bytes, Error = std::io::Error> + Unpin,
    ) {
        let req = protocol::request(protocol::Opcode::Hello, 0, 0, 0, 0, None, None, None);
        output.send(req.freeze()).await.unwrap();

        println!("--> {:?}", input.next().await);
    }
}

#[cfg(test)]
mod tests {

    use super::KvEndpoint;

    #[tokio::test]
    async fn my_test() {
        let endpoint = KvEndpoint::new("127.0.0.1".into(), 11210);
        endpoint.connect().await;
    }
}
