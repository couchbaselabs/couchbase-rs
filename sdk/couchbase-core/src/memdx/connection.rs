use crate::memdx::error::ErrorKind;
use crate::memdx::error::Result;
use crate::tls_config::TlsConfig;
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::{timeout_at, Instant};
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    tokio_rustls::rustls::pki_types::{IpAddr, ServerName},
    tokio_rustls::TlsConnector,
};

#[derive(Debug)]
pub struct ConnectOptions {
    pub deadline: Instant,
}

pub trait Stream: Debug + AsyncWrite + AsyncRead + Send + Sync + Unpin + 'static {}

impl Stream for TcpStream {}

#[derive(Debug)]
pub enum ConnectionType {
    Tcp(TcpConnection),
    Tls(TlsConnection),
}

impl ConnectionType {
    pub fn into_inner(self) -> Box<dyn Stream> {
        match self {
            ConnectionType::Tcp(connection) => Box::new(connection.stream),
            ConnectionType::Tls(connection) => Box::new(connection.stream),
        }
    }

    pub fn local_addr(&self) -> &Option<SocketAddr> {
        match self {
            ConnectionType::Tcp(connection) => &connection.local_addr,
            ConnectionType::Tls(connection) => &connection.local_addr,
        }
    }

    pub fn peer_addr(&self) -> &Option<SocketAddr> {
        match self {
            ConnectionType::Tcp(connection) => &connection.peer_addr,
            ConnectionType::Tls(connection) => &connection.peer_addr,
        }
    }
}

#[derive(Debug)]
pub struct TcpConnection {
    stream: TcpStream,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl TcpConnection {
    pub async fn connect(addr: SocketAddr, opts: ConnectOptions) -> Result<TcpConnection> {
        let remote_addr = addr.to_string();

        let stream = timeout_at(opts.deadline, TcpStream::connect(remote_addr))
            .await?
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;
        stream
            .set_nodelay(false)
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        let local_addr = match stream.local_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };
        let peer_addr = match stream.peer_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };

        Ok(TcpConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    fn local_addr(&self) -> &Option<SocketAddr> {
        &self.local_addr
    }

    fn peer_addr(&self) -> &Option<SocketAddr> {
        &self.peer_addr
    }
}

#[derive(Debug)]
pub struct TlsConnection {
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    stream: tokio_rustls::client::TlsStream<TcpStream>,
    #[cfg(feature = "native-tls")]
    stream: tokio_native_tls::TlsStream<TcpStream>,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl Stream for tokio_rustls::client::TlsStream<TcpStream> {}

#[cfg(feature = "native-tls")]
impl Stream for tokio_native_tls::TlsStream<TcpStream> {}

impl TlsConnection {
    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub async fn connect(
        addr: SocketAddr,
        tls_config: TlsConfig,
        opts: ConnectOptions,
    ) -> Result<TlsConnection> {
        let remote_addr = addr.to_string();

        let tcp_socket = timeout_at(opts.deadline, TcpStream::connect(remote_addr))
            .await?
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        tcp_socket
            .set_nodelay(false)
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        let local_addr = match tcp_socket.local_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };
        let peer_addr = match tcp_socket.peer_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };

        let connector = TlsConnector::from(tls_config);

        let stream = timeout_at(
            opts.deadline,
            connector.connect(ServerName::IpAddress(IpAddr::from(addr.ip())), tcp_socket),
        )
        .await?
        .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        Ok(TlsConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    #[cfg(feature = "native-tls")]
    pub async fn connect(
        addr: SocketAddr,
        tls_config: TlsConfig,
        opts: ConnectOptions,
    ) -> Result<TlsConnection> {
        let remote_addr = addr.to_string();

        let tcp_socket = timeout_at(opts.deadline, TcpStream::connect(&remote_addr))
            .await?
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        tcp_socket
            .set_nodelay(false)
            .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        let local_addr = match tcp_socket.local_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };
        let peer_addr = match tcp_socket.peer_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };

        let tls_connector = tokio_native_tls::TlsConnector::from(tls_config);

        let stream = timeout_at(
            opts.deadline,
            tls_connector.connect(&remote_addr, tcp_socket),
        )
        .await?
        .map_err(|e| ErrorKind::Connect { msg: e.to_string() })?;

        Ok(TlsConnection {
            stream,
            local_addr,
            peer_addr,
        })
    }

    fn local_addr(&self) -> &Option<SocketAddr> {
        &self.local_addr
    }

    fn peer_addr(&self) -> &Option<SocketAddr> {
        &self.peer_addr
    }
}
