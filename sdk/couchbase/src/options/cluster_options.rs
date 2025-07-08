use crate::authenticator::Authenticator;
use couchbase_core::ondemand_agentmanager::OnDemandAgentManagerOptions;
use couchbase_core::options::agent::CompressionConfig;
pub use couchbase_core::options::agent::CompressionMode;
use log::debug;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use crate::capella_ca::CAPELLA_CERT;
use crate::error;
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    couchbase_core::insecure_certverfier::InsecureCertVerifier, rustls_pemfile::read_all,
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider,
    tokio_rustls::rustls::pki_types::CertificateDer, tokio_rustls::rustls::RootCertStore,
    webpki_roots::TLS_SERVER_ROOTS,
};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterOptions {
    // authenticator specifies the authenticator to use with the cluster.
    pub(crate) authenticator: Authenticator,
    // timeout_options specifies various operation timeouts.
    pub(crate) timeout_options: Option<TimeoutOptions>,
    // compression_mode specifies compression related configuration options.
    pub(crate) compression_mode: Option<CompressionMode>,
    pub(crate) tls_options: Option<TlsOptions>,
}

impl ClusterOptions {
    pub fn new(authenticator: Authenticator) -> Self {
        Self {
            authenticator,
            timeout_options: None,
            compression_mode: None,
            tls_options: None,
        }
    }

    pub fn timeout_options(mut self, timeout_options: TimeoutOptions) -> Self {
        self.timeout_options = Some(timeout_options);
        self
    }

    pub fn compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = Some(compression_mode);
        self
    }

    pub fn tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct TimeoutOptions {
    pub(crate) kv_connect_timeout: Option<Duration>,
}

impl TimeoutOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn kv_connect_timeout(mut self, timeout: Duration) -> Self {
        self.kv_connect_timeout = Some(timeout);
        self
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
#[non_exhaustive]
pub struct TlsOptions {
    pub(crate) danger_accept_invalid_certs: Option<bool>,
    pub(crate) ca_certificate: Option<Vec<u8>>,

    #[cfg(feature = "native-tls")]
    pub(crate) danger_accept_invalid_hostnames: Option<bool>,
}

impl TlsOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn danger_accept_invalid_certs(mut self, danger: bool) -> Self {
        self.danger_accept_invalid_certs = Some(danger);
        self
    }

    pub fn ca_certificate(mut self, cert: Vec<u8>) -> Self {
        self.ca_certificate = Some(cert);
        self
    }

    #[cfg(feature = "native-tls")]
    pub fn danger_accept_invalid_hostnames(mut self, danger: bool) -> Self {
        self.danger_accept_invalid_hostnames = Some(danger);
        self
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl TryFrom<TlsOptions> for Arc<tokio_rustls::rustls::ClientConfig> {
    type Error = error::Error;

    fn try_from(opts: TlsOptions) -> Result<Self, Self::Error> {
        let store = if let Some(ca_cert) = opts.ca_certificate {
            let mut store = RootCertStore::empty();
            let mut cursor = Cursor::new(ca_cert);
            let certs = rustls_pemfile::certs(&mut cursor)
                .map(|item| item.map_err(|e| error::Error::other_failure(e.to_string())))
                .collect::<error::Result<Vec<CertificateDer>>>()?;

            store.add_parsable_certificates(certs);
            store
        } else {
            let mut store = RootCertStore {
                roots: TLS_SERVER_ROOTS.to_vec(),
            };

            debug!("Adding Capella root CA to trust store");
            let mut cursor = Cursor::new(CAPELLA_CERT);
            let certs = rustls_pemfile::certs(&mut cursor)
                .map(|item| item.map_err(|e| error::Error::other_failure(e.to_string())))
                .collect::<error::Result<Vec<CertificateDer>>>()?;

            store.add_parsable_certificates(certs);
            store
        };

        let mut config =
            tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| error::Error::other_failure(e.to_string()))?;

        let config = if let Some(true) = opts.danger_accept_invalid_certs {
            config
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
                .with_no_client_auth()
        } else {
            config.with_root_certificates(store).with_no_client_auth()
        };

        Ok(Arc::new(config))
    }
}

#[cfg(feature = "native-tls")]
impl TryFrom<TlsOptions> for tokio_native_tls::native_tls::TlsConnector {
    type Error = error::Error;

    fn try_from(opts: TlsOptions) -> Result<Self, Self::Error> {
        let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();
        if let Some(true) = opts.danger_accept_invalid_certs {
            builder.danger_accept_invalid_certs(true);
        }
        if let Some(true) = opts.danger_accept_invalid_hostnames {
            builder.danger_accept_invalid_hostnames(true);
        }
        if let Some(cert) = opts.ca_certificate {
            let pem = tokio_native_tls::native_tls::Certificate::from_pem(&cert)
                .map_err(|e| error::Error::other_failure(e.to_string()))?;
            builder.add_root_certificate(pem);
        } else {
            debug!("Adding Capella root CA to trust store");
            let capella_ca =
                tokio_native_tls::native_tls::Certificate::from_pem(CAPELLA_CERT.as_ref())
                    .map_err(|e| error::Error::other_failure(e.to_string()))?;
            builder.add_root_certificate(capella_ca);
        }
        builder
            .build()
            .map_err(|e| error::Error::other_failure(e.to_string()))
    }
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            kv_connect_timeout: Some(Duration::from_secs(10)),
        }
    }
}
