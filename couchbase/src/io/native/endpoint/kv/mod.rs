mod codec;
mod protocol;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use codec::KeyValueCodec;
use futures::pin_mut;
use futures::sink::SinkExt;
use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use serde_derive::Deserialize;
use std::convert::TryFrom;
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
        let mut output = FramedWrite::new(w, KeyValueCodec::new());
        let input = FramedRead::new(r, KeyValueCodec::new());

        pin_mut!(input);

        send_hello(&mut output).await.unwrap();
        send_error_map(&mut output).await.unwrap();

        let features = receive_hello(&mut input).await.unwrap();
        let error_map = receive_error_map(&mut input).await.unwrap();

        println!("Negotiated features {:?}", features);
        println!("Error Map: {:?}", error_map);
    }
}

async fn send_hello(
    output: &mut (impl Sink<Bytes, Error = IoError> + Unpin),
) -> Result<(), IoError> {
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
    Ok(())
}

async fn receive_hello(
    input: &mut Pin<&mut impl Stream<Item = Result<BytesMut, IoError>>>,
) -> Result<Vec<ServerFeature>, IoError> {
    let response = input.next().await.unwrap()?.freeze();

    let mut features = vec![];
    if let Some(mut body) = protocol::body(&response) {
        while body.remaining() > 0 {
            if let Ok(f) = ServerFeature::try_from(body.get_u16()) {
                features.push(f);
            } else {
                // todo: debug that we got an unknown server feature
            }
        }
    }

    Ok(features)
}

async fn send_error_map(
    output: &mut (impl Sink<Bytes, Error = IoError> + Unpin),
) -> Result<(), IoError> {
    let mut body = BytesMut::with_capacity(2);
    body.put_u16(protocol::ERROR_MAP_VERSION);

    let req = protocol::request(
        protocol::Opcode::ErrorMap,
        0,
        0,
        0,
        0,
        None,
        None,
        Some(body.freeze()),
    );
    output.send(req.freeze()).await?;
    Ok(())
}

async fn receive_error_map(
    input: &mut Pin<&mut impl Stream<Item = Result<BytesMut, IoError>>>,
) -> Result<ErrorMap, IoError> {
    let response = input.next().await.unwrap()?.freeze();
    if let Some(mut body) = protocol::body(&response) {
        let error_map = serde_json::from_slice(body.bytes()).unwrap();
        return Ok(error_map);
    }
    panic!("Unhandled IO Error");
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

impl TryFrom<u16> for ServerFeature {
    type Error = u16;

    fn try_from(input: u16) -> Result<Self, Self::Error> {
        Ok(match input {
            0x08 => Self::SelectBucket,
            0x06 => Self::Xattr,
            0x07 => Self::Xerror,
            0x10 => Self::AltRequest,
            0x11 => Self::SyncReplication,
            0x12 => Self::Collections,
            0x0F => Self::Tracing,
            0x04 => Self::MutationSeqno,
            0x0A => Self::Snappy,
            0x0E => Self::UnorderedExecution,
            0x15 => Self::Vattr,
            0x17 => Self::CreateAsDeleted,
            _ => return Err(input),
        })
    }
}

#[derive(Debug, Deserialize)]
struct ErrorMap {
    version: u16,
    revision: u16,
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
