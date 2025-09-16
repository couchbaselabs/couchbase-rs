use crate::authenticator::Authenticator;
use crate::capella_ca::CAPELLA_CERT;
use crate::error;
use log::debug;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Identity;

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use {
    couchbase_core::insecure_certverfier::InsecureCertVerifier,
    rustls_pemfile::read_all,
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider,
    tokio_rustls::rustls::pki_types::pem::{PemObject, SectionKind},
    tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer},
    tokio_rustls::rustls::RootCertStore,
    webpki_roots::TLS_SERVER_ROOTS,
};

#[derive(Clone)]
#[non_exhaustive]
pub struct ClusterOptions {
    // authenticator specifies the authenticator to use with the cluster.
    pub(crate) authenticator: Authenticator,
    // timeout_options specifies various operation timeouts.
    // compression_mode specifies compression related configuration options.
    pub(crate) compression_mode: Option<CompressionMode>,
    pub(crate) tls_options: Option<TlsOptions>,
    pub(crate) tcp_keep_alive_time: Option<Duration>,
    pub(crate) poller_options: PollerOptions,
    pub(crate) http_options: HttpOptions,
    pub(crate) kv_options: KvOptions,
    pub(crate) dns_options: Option<DnsOptions>,
    pub(crate) orphan_reporter_options: OrphanReporterOptions,
}

impl Debug for ClusterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterOptions")
            .field("authenticator", &self.authenticator)
            .field("compression_mode", &self.compression_mode)
            .field("has_tls_options", &self.tls_options.is_some())
            .field("tcp_keep_alive_time", &self.tcp_keep_alive_time)
            .field("poller_options", &self.poller_options)
            .field("http_options", &self.http_options)
            .field("kv_options", &self.kv_options)
            .field("orphan_reporter_options", &self.orphan_reporter_options)
            .finish()
    }
}

impl ClusterOptions {
    pub fn new(authenticator: Authenticator) -> Self {
        Self {
            authenticator,
            compression_mode: None,
            tls_options: None,
            tcp_keep_alive_time: None,
            poller_options: PollerOptions::new(),
            http_options: HttpOptions::new(),
            kv_options: KvOptions::new(),
            dns_options: None,
            orphan_reporter_options: OrphanReporterOptions::new(),
        }
    }

    pub fn compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = Some(compression_mode);
        self
    }

    pub fn tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }

    pub fn tcp_keep_alive_time(mut self, val: Duration) -> Self {
        self.tcp_keep_alive_time = Some(val);
        self
    }

    pub fn poller_options(mut self, poller_options: PollerOptions) -> Self {
        self.poller_options = poller_options;
        self
    }

    pub fn http_options(mut self, http_options: HttpOptions) -> Self {
        self.http_options = http_options;
        self
    }

    pub fn kv_options(mut self, kv_options: KvOptions) -> Self {
        self.kv_options = kv_options;
        self
    }

    #[cfg(feature = "volatile")]
    pub fn dns_options(mut self, dns_options: DnsOptions) -> Self {
        self.dns_options = Some(dns_options);
        self
    }

    pub fn orphan_reporter_options(
        mut self,
        orphan_reporter_options: OrphanReporterOptions,
    ) -> Self {
        self.orphan_reporter_options = orphan_reporter_options;
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompressionMode {
    Enabled { min_size: usize, min_ratio: f64 },
    Disabled,
}

impl From<CompressionMode> for couchbase_core::options::agent::CompressionMode {
    fn from(mode: CompressionMode) -> Self {
        match mode {
            CompressionMode::Enabled {
                min_size,
                min_ratio,
            } => couchbase_core::options::agent::CompressionMode::Enabled {
                min_size,
                min_ratio,
            },
            CompressionMode::Disabled => couchbase_core::options::agent::CompressionMode::Disabled,
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct PollerOptions {
    pub(crate) config_poll_interval: Option<Duration>,
}

impl PollerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config_poll_interval(mut self, interval: Duration) -> Self {
        self.config_poll_interval = Some(interval);
        self
    }
}

impl From<PollerOptions> for couchbase_core::options::agent::ConfigPollerConfig {
    fn from(opts: PollerOptions) -> Self {
        let mut core_opts = couchbase_core::options::agent::ConfigPollerConfig::default();
        if let Some(interval) = opts.config_poll_interval {
            core_opts = core_opts.poll_interval(interval);
        }

        core_opts
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct HttpOptions {
    pub(crate) max_idle_connections_per_host: Option<usize>,
    pub(crate) idle_connection_timeout: Option<Duration>,
}

impl HttpOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_idle_connections_per_host(mut self, max: usize) -> Self {
        self.max_idle_connections_per_host = Some(max);
        self
    }

    pub fn idle_connection_timeout(mut self, timeout: Duration) -> Self {
        self.idle_connection_timeout = Some(timeout);
        self
    }
}

impl From<HttpOptions> for couchbase_core::options::agent::HttpConfig {
    fn from(opts: HttpOptions) -> Self {
        let mut core_opts = couchbase_core::options::agent::HttpConfig::default();
        if let Some(max) = opts.max_idle_connections_per_host {
            core_opts = core_opts.max_idle_connections_per_host(max);
        }

        if let Some(timeout) = opts.idle_connection_timeout {
            core_opts = core_opts.idle_connection_timeout(timeout);
        }

        core_opts
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct KvOptions {
    pub(crate) enable_mutation_tokens: Option<bool>,
    pub(crate) enable_server_durations: Option<bool>,
    pub(crate) num_connections: Option<usize>,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) connect_throttle_timeout: Option<Duration>,
}

impl KvOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable_mutation_tokens(mut self, enable: bool) -> Self {
        self.enable_mutation_tokens = Some(enable);
        self
    }

    pub fn enable_server_durations(mut self, enable: bool) -> Self {
        self.enable_server_durations = Some(enable);
        self
    }

    pub fn num_connections(mut self, num: usize) -> Self {
        self.num_connections = Some(num);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn connect_throttle_timeout(mut self, timeout: Duration) -> Self {
        self.connect_throttle_timeout = Some(timeout);
        self
    }
}

impl From<KvOptions> for couchbase_core::options::agent::KvConfig {
    fn from(opts: KvOptions) -> Self {
        let mut core_opts = couchbase_core::options::agent::KvConfig::default();
        if let Some(enable) = opts.enable_mutation_tokens {
            core_opts = core_opts.enable_mutation_tokens(enable);
        }

        if let Some(enable) = opts.enable_server_durations {
            core_opts = core_opts.enable_server_durations(enable);
        }

        if let Some(num) = opts.num_connections {
            core_opts = core_opts.num_connections(num);
        }

        if let Some(timeout) = opts.connect_timeout {
            core_opts = core_opts.connect_timeout(timeout);
        }

        if let Some(timeout) = opts.connect_throttle_timeout {
            core_opts = core_opts.connect_throttle_timeout(timeout);
        }

        core_opts
    }
}

#[derive(Clone, Default)]
#[non_exhaustive]
pub struct TlsOptions {
    pub(crate) danger_accept_invalid_certs: Option<bool>,

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub(crate) ca_certificate: Option<CertificateDer<'static>>,

    #[cfg(feature = "native-tls")]
    pub(crate) ca_certificate: Option<tokio_native_tls::native_tls::Certificate>,

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

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub fn ca_certificate(mut self, cert: CertificateDer<'static>) -> Self {
        self.ca_certificate = Some(cert);
        self
    }

    #[cfg(feature = "native-tls")]
    pub fn ca_certificate(mut self, cert: tokio_native_tls::native_tls::Certificate) -> Self {
        self.ca_certificate = Some(cert);
        self
    }

    #[cfg(feature = "native-tls")]
    pub fn danger_accept_invalid_hostnames(mut self, danger: bool) -> Self {
        self.danger_accept_invalid_hostnames = Some(danger);
        self
    }

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub(crate) fn try_into_tls_config(
        self,
        auth: &Authenticator,
    ) -> Result<Arc<tokio_rustls::rustls::ClientConfig>, error::Error> {
        let store = if let Some(ca_cert) = self.ca_certificate {
            let mut store = RootCertStore::empty();
            store
                .add(ca_cert)
                .map_err(|e| error::Error::other_failure(format!("failed to add cert: {e}")))?;
            store
        } else {
            let mut store = RootCertStore {
                roots: TLS_SERVER_ROOTS.to_vec(),
            };

            debug!("Adding Capella root CA to trust store");
            let mut cursor = Cursor::new(CAPELLA_CERT);
            let certs = rustls_pemfile::certs(&mut cursor)
                .map(|item| {
                    item.map_err(|e| {
                        error::Error::other_failure(format!("failed to add capella cert: {e}"))
                    })
                })
                .collect::<error::Result<Vec<CertificateDer>>>()?;

            store.add_parsable_certificates(certs);
            store
        };

        let mut builder =
            tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    error::Error::other_failure(format!(
                        "failed to set safe default protocol versions: {e}"
                    ))
                })?;

        let builder = if let Some(true) = self.danger_accept_invalid_certs {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
        } else {
            builder.with_root_certificates(store)
        };

        let config = match auth {
            Authenticator::CertificateAuthenticator(a) => {
                let clone = a.clone();
                builder
                    .with_client_auth_cert(clone.cert_chain, clone.private_key)
                    .map_err(|e| {
                        error::Error::other_failure(format!(
                            "failed to setup client auth cert: {e}"
                        ))
                    })?
            }
            Authenticator::PasswordAuthenticator(_) => builder.with_no_client_auth(),
        };

        Ok(Arc::new(config))
    }

    #[cfg(feature = "native-tls")]
    pub(crate) fn try_into_tls_config(
        self,
        auth: &Authenticator,
    ) -> Result<tokio_native_tls::native_tls::TlsConnector, error::Error> {
        let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();
        if let Some(true) = self.danger_accept_invalid_certs {
            builder.danger_accept_invalid_certs(true);
        }
        if let Some(true) = self.danger_accept_invalid_hostnames {
            builder.danger_accept_invalid_hostnames(true);
        }
        if let Some(cert) = self.ca_certificate {
            builder.add_root_certificate(cert);
        } else {
            debug!("Adding Capella root CA to trust store");
            let capella_ca =
                tokio_native_tls::native_tls::Certificate::from_pem(CAPELLA_CERT.as_ref())
                    .map_err(|e| {
                        error::Error::other_failure(format!("failed to add capella cert: {e}"))
                    })?;
            builder.add_root_certificate(capella_ca);
        }

        match auth {
            Authenticator::CertificateAuthenticator(a) => {
                builder.identity(a.identity.clone());
            }
            Authenticator::PasswordAuthenticator(_) => {}
        };

        builder
            .build()
            .map_err(|e| error::Error::other_failure(format!("failed to build client config: {e}")))
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct DnsOptions {
    pub(crate) namespace: SocketAddr,
    pub(crate) timeout: Option<Duration>,
}
impl DnsOptions {
    pub fn new(namespace: SocketAddr) -> Self {
        Self {
            namespace,
            timeout: None,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
impl From<DnsOptions> for couchbase_connstr::DnsConfig {
    fn from(opts: DnsOptions) -> Self {
        couchbase_connstr::DnsConfig {
            namespace: opts.namespace,
            timeout: opts.timeout,
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct OrphanReporterOptions {
    pub(crate) enabled: Option<bool>,
    pub(crate) reporter_interval: Option<Duration>,
    pub(crate) sample_size: Option<usize>,
}

impl OrphanReporterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    pub fn reporter_interval(mut self, reporter_interval: Duration) -> Self {
        self.reporter_interval = Some(reporter_interval);
        self
    }

    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = Some(sample_size);
        self
    }
}

impl From<OrphanReporterOptions>
    for couchbase_core::options::orphan_reporter::OrphanReporterConfig
{
    fn from(opts: OrphanReporterOptions) -> Self {
        let mut core_opts =
            couchbase_core::options::orphan_reporter::OrphanReporterConfig::default();

        if let Some(reporter_interval) = opts.reporter_interval {
            core_opts = core_opts.reporter_interval(reporter_interval);
        }

        if let Some(sample_size) = opts.sample_size {
            core_opts = core_opts.sample_size(sample_size);
        }

        core_opts
    }
}
