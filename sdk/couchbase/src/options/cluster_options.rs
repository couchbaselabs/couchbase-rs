use crate::authenticator::Authenticator;
use couchbase_core::agentoptions::CompressionConfig;
pub use couchbase_core::agentoptions::CompressionMode;
use couchbase_core::ondemand_agentmanager::OnDemandAgentManagerOptions;
use log::debug;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use typed_builder::TypedBuilder;

use crate::capella_ca::CAPELLA_CERT;
use crate::error;
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    couchbase_core::insecure_certverfier::InsecureCertVerifier, rustls_pemfile::read_all,
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider,
    tokio_rustls::rustls::pki_types::CertificateDer, tokio_rustls::rustls::RootCertStore,
    webpki_roots::TLS_SERVER_ROOTS,
};

#[derive(Clone, Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ClusterOptions {
    // authenticator specifies the authenticator to use with the cluster.
    #[builder(!default)]
    pub authenticator: Authenticator,
    // timeout_options specifies various operation timeouts.
    pub timeout_options: TimeoutOptions,
    // compression_mode specifies compression related configuration options.
    pub compression_mode: CompressionMode,
    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
    pub tls_options: Option<TlsOptions>,
}

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TimeoutOptions {
    pub kv_connect_timeout: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TlsOptions {
    pub danger_accept_invalid_certs: Option<bool>,
    pub ca_certificate: Option<Vec<u8>>,

    #[cfg(feature = "native-tls")]
    pub danger_accept_invalid_hostnames: Option<bool>,
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl TryFrom<TlsOptions> for Arc<tokio_rustls::rustls::ClientConfig> {
    type Error = error::Error;

    fn try_from(opts: TlsOptions) -> Result<Self, Self::Error> {
        let store = if let Some(ca_cert) = opts.ca_certificate {
            let mut store = RootCertStore::empty();
            let mut cursor = Cursor::new(ca_cert);
            let certs = rustls_pemfile::certs(&mut cursor)
                .map(|item| item.map_err(|e| error::Error { msg: e.to_string() }))
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
                .map(|item| item.map_err(|e| error::Error { msg: e.to_string() }))
                .collect::<error::Result<Vec<CertificateDer>>>()?;

            store.add_parsable_certificates(certs);
            store
        };

        let mut config =
            tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| error::Error { msg: e.to_string() })?;

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
                .map_err(|e| error::Error { msg: e.to_string() })?;
            builder.add_root_certificate(pem);
        } else {
            debug!("Adding Capella root CA to trust store");
            let capella_ca =
                tokio_native_tls::native_tls::Certificate::from_pem(CAPELLA_CERT.as_ref())
                    .map_err(|e| error::Error { msg: e.to_string() })?;
            builder.add_root_certificate(capella_ca);
        }
        builder
            .build()
            .map_err(|e| error::Error { msg: e.to_string() })
    }
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            kv_connect_timeout: Some(Duration::from_secs(10)),
        }
    }
}

impl TryFrom<ClusterOptions> for OnDemandAgentManagerOptions {
    type Error = error::Error;

    fn try_from(opts: ClusterOptions) -> Result<Self, Self::Error> {
        let tls_config = if let Some(tls_config) = opts.tls_options {
            Some(tls_config.try_into().map_err(|e| error::Error {
                msg: format!("{:?}", e),
            })?)
        } else {
            None
        };
        let builder = OnDemandAgentManagerOptions::builder()
            .authenticator(opts.authenticator)
            .connect_timeout(
                opts.timeout_options
                    .kv_connect_timeout
                    .unwrap_or(Duration::from_secs(10)),
            )
            .compression_config(
                CompressionConfig::builder()
                    .mode(opts.compression_mode)
                    .build(),
            )
            .tls_config(tls_config);

        Ok(builder.build())
    }
}
