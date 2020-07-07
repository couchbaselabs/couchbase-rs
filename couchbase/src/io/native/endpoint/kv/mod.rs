mod codec;
mod protocol;

use bytes::{BufMut, Bytes, BytesMut};
use codec::KeyValueCodec;
use futures::pin_mut;
use futures::sink::SinkExt;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use std::io::Error as IoError;
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
        let output = FramedWrite::new(w, KeyValueCodec::new());
        let input = FramedRead::new(r, KeyValueCodec::new());

        pin_mut!(input);

        let features = negotiate_hello(input, output).await.unwrap();
        println!("Negotiated features {:?}", features);
    }

}

async fn negotiate_hello(
    mut input: Pin<&mut dyn Stream<Item = Result<BytesMut, IoError>>>,
    mut output: impl Sink<Bytes, Error = IoError> + Unpin,
) -> Result<Vec<ServerFeature>, IoError> {
    let features = vec![
        ServerFeature::SelectBucket,
        ServerFeature::Xattr,
        ServerFeature::Xerror,
        ServerFeature::AltRequest,
        ServerFeature::SyncReplication,
        ServerFeature::Collections,
        ServerFeature::Tracing,
        ServerFeature::UnorderedExecution,
    ];
    let mut body = BytesMut::with_capacity(features.len() * 2);
    for feature in &features {
        body.put_u16(feature.encoded());
    }

    let req = protocol::request(
        protocol::Opcode::Hello,
        0,
        0,
        0,
        0,
        None,
        None,
        Some(body.freeze()),
    );
    output.send(req.freeze()).await?;

    let response = input.next().await.unwrap()?;

    println!("--> {:?}", response);
    Ok(vec![])
}


#[derive(Debug)]
enum ServerFeature {
    SelectBucket,
    Xattr,
    Xerror,
    AltRequest,
    SyncReplication,
    Collections,
    Tracing,
    MutationSeqno,
    Snappy,
    UnorderedExecution,
    Vattr,
    CreateAsDeleted,
}

impl ServerFeature {
    pub fn encoded(&self) -> u16 {
        match self {
            Self::SelectBucket => 0x08,
            Self::Xattr => 0x06,
            Self::Xerror => 0x07,
            Self::AltRequest => 0x10,
            Self::SyncReplication => 0x11,
            Self::Collections => 0x12,
            Self::Tracing => 0x0F,
            Self::MutationSeqno => 0x04,
            Self::Snappy => 0x0A,
            Self::UnorderedExecution => 0x0E,
            Self::Vattr => 0x15,
            Self::CreateAsDeleted => 0x17,
        }
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
