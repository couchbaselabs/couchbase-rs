use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::TlsConnector;

use crate::memdx::client::Result;
use crate::memdx::error::Error;

#[derive(Debug, Default)]
pub struct TlsConfig {
    pub root_certs: Option<RootCertStore>,
    pub accept_all_certs: Option<bool>,
}

#[derive(Debug, Default)]
pub struct ConnectOptions {
    pub tls_config: Option<TlsConfig>,
}

#[derive(Debug)]
pub enum ConnectionType {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

#[derive(Debug)]
pub struct Connection {
    inner: ConnectionType,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl Connection {
    pub async fn connect(
        hostname: impl Into<String>,
        port: u32,
        opts: ConnectOptions,
    ) -> Result<Connection> {
        let hostname = hostname.into();
        let remote_addr = format!("{}:{}", hostname, port);

        if let Some(tls_config) = opts.tls_config {
            let builder = ClientConfig::builder();
            let config = if tls_config.accept_all_certs.unwrap_or_default() {
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
                    .with_no_client_auth()
            } else if let Some(roots) = tls_config.root_certs {
                builder.with_root_certificates(roots).with_no_client_auth()
            } else {
                return Err(Error::Generic(
                    "If tls config is specified then roots or accept_all_certs must be specified"
                        .to_string(),
                ));
            };

            let tcp_socket = TcpStream::connect(remote_addr)
                .await
                .map_err(|e| Error::Connect(e.kind()))?;

            tcp_socket
                .set_nodelay(false)
                .map_err(|e| Error::Connect(e.kind()))?;

            let local_addr = match tcp_socket.local_addr() {
                Ok(addr) => Some(addr),
                Err(_) => None,
            };
            let peer_addr = match tcp_socket.peer_addr() {
                Ok(addr) => Some(addr),
                Err(_) => None,
            };

            let connector = TlsConnector::from(Arc::new(config));
            let socket = connector
                .connect(
                    ServerName::try_from(hostname).map_err(|e| Error::Generic(e.to_string()))?,
                    tcp_socket,
                )
                .await
                .map_err(|e| Error::Connect(e.kind()))?;

            Ok(Connection {
                inner: ConnectionType::Tls(socket),
                local_addr,
                peer_addr,
            })
        } else {
            let socket = TcpStream::connect(remote_addr)
                .await
                .map_err(|e| Error::Connect(e.kind()))?;
            socket
                .set_nodelay(false)
                .map_err(|e| Error::Connect(e.kind()))?;

            let local_addr = match socket.local_addr() {
                Ok(addr) => Some(addr),
                Err(_) => None,
            };
            let peer_addr = match socket.peer_addr() {
                Ok(addr) => Some(addr),
                Err(_) => None,
            };

            Ok(Connection {
                inner: ConnectionType::Tcp(socket),
                local_addr,
                peer_addr,
            })
        }
    }

    pub fn local_addr(&self) -> &Option<SocketAddr> {
        &self.local_addr
    }

    pub fn peer_addr(&self) -> &Option<SocketAddr> {
        &self.peer_addr
    }

    pub fn into_inner(self) -> ConnectionType {
        self.inner
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
