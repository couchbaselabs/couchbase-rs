use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::aws_lc_rs::default_provider;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::{CertificateDer, IpAddr, ServerName, UnixTime};
use tokio_rustls::TlsConnector;

use crate::memdx::error::ErrorKind;
use crate::memdx::error::Result;

#[derive(Debug, Default, Clone)]
pub struct TlsConfig {
    pub root_certs: Option<RootCertStore>,
    pub accept_all_certs: Option<bool>,
}

impl PartialEq for TlsConfig {
    fn eq(&self, other: &Self) -> bool {
        if self.accept_all_certs != other.accept_all_certs {
            return false;
        }
        if self.root_certs.is_some() != other.root_certs.is_some() {
            return false;
        }

        if let Some(certs) = &self.root_certs {
            if let Some(other_certs) = &other.root_certs {
                if certs.roots.len() != other_certs.roots.len() {
                    return false;
                }
                let other_roots = &other_certs.roots;
                for cert in &certs.roots {
                    if !other_roots.contains(cert) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        true
    }
}

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
            .map_err(|e| ErrorKind::Connect(Arc::new(e)))?;
        stream
            .set_nodelay(false)
            .map_err(|e| ErrorKind::Connect(Arc::new(e)))?;

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
    stream: TlsStream<TcpStream>,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl Stream for TlsStream<TcpStream> {}

impl TlsConnection {
    pub async fn connect(
        addr: SocketAddr,
        tls_config: TlsConfig,
        opts: ConnectOptions,
    ) -> Result<TlsConnection> {
        let remote_addr = addr.to_string();

        let _ = CryptoProvider::install_default(default_provider());
        let builder = ClientConfig::builder();

        let config = if tls_config.accept_all_certs.unwrap_or_default() {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
                .with_no_client_auth()
        } else if let Some(roots) = tls_config.root_certs {
            builder.with_root_certificates(roots).with_no_client_auth()
        } else {
            return Err(ErrorKind::InvalidArgument {
                msg: "If tls config is specified then roots or accept_all_certs must be specified"
                    .to_string(),
            }
            .into());
        };

        let tcp_socket = timeout_at(opts.deadline, TcpStream::connect(remote_addr))
            .await?
            .map_err(|e| ErrorKind::Connect(Arc::new(e)))?;

        tcp_socket
            .set_nodelay(false)
            .map_err(|e| ErrorKind::Connect(Arc::new(e)))?;

        let local_addr = match tcp_socket.local_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };
        let peer_addr = match tcp_socket.peer_addr() {
            Ok(addr) => Some(addr),
            Err(_) => None,
        };

        // TODO: This should probably just be passed around as an arc anyway.
        let connector = TlsConnector::from(Arc::new(config));
        let stream = timeout_at(
            opts.deadline,
            connector.connect(ServerName::IpAddress(IpAddr::from(addr.ip())), tcp_socket),
        )
        .await?
        .map_err(|e| ErrorKind::Connect(Arc::new(e)))?;

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

#[derive(Debug)]
struct InsecureCertVerifier {}

impl ServerCertVerifier for InsecureCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}
