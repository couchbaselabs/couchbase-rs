mod protocol;
mod codec;

use std::net::SocketAddr;
use tokio::net::TcpStream;

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
