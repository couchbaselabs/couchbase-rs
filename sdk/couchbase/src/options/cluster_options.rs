/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::authenticator::Authenticator;
use crate::capella_ca::CAPELLA_CERT;
use crate::error;
use log::debug;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
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
    pub authenticator: Authenticator,
    // timeout_options specifies various operation timeouts.
    // compression_mode specifies compression related configuration options.
    pub compression_mode: Option<CompressionMode>,
    pub tls_options: Option<TlsOptions>,
    pub tcp_keep_alive_time: Option<Duration>,
    pub poller_options: PollerOptions,
    pub http_options: HttpOptions,
    pub kv_options: KvOptions,
    #[cfg(feature = "unstable-dns-options")]
    pub dns_options: Option<DnsOptions>,
    pub orphan_reporter_options: OrphanReporterOptions,
    pub default_retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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
            #[cfg(feature = "unstable-dns-options")]
            dns_options: None,
            orphan_reporter_options: OrphanReporterOptions::new(),
            default_retry_strategy: None,
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

    #[cfg(feature = "unstable-dns-options")]
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

    pub fn default_retry_strategy(
        mut self,
        retry_strategy: Arc<dyn crate::retry::RetryStrategy>,
    ) -> Self {
        self.default_retry_strategy = Some(retry_strategy);
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

impl Display for CompressionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CompressionMode::Enabled {
                min_size,
                min_ratio,
            } => {
                write!(f, "{{ min_size: {min_size}, min_ratio: {min_ratio} }}")
            }
            CompressionMode::Disabled => write!(f, "disabled"),
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct PollerOptions {
    pub config_poll_interval: Option<Duration>,
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

impl Display for PollerOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ config_poll_interval: {:?} }}",
            self.config_poll_interval
        )
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HttpOptions {
    pub max_idle_connections_per_host: Option<usize>,
    pub idle_connection_timeout: Option<Duration>,
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

impl Display for HttpOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ max_idle_connections_per_host: {:?}, idle_connection_timeout: {:?} }}",
            self.max_idle_connections_per_host, self.idle_connection_timeout
        )
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct KvOptions {
    pub enable_mutation_tokens: Option<bool>,
    pub enable_server_durations: Option<bool>,
    pub num_connections: Option<usize>,
    pub connect_timeout: Option<Duration>,
    pub connect_throttle_timeout: Option<Duration>,
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
        let mut core_opts =
            couchbase_core::options::agent::KvConfig::default().enable_error_map(true);
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

impl Display for KvOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ enable_mutation_tokens: {:?}, enable_server_durations: {:?}, num_connections: {:?}, connect_timeout: {:?}, connect_throttle_timeout: {:?} }}",
            self.enable_mutation_tokens,
            self.enable_server_durations,
            self.num_connections,
            self.connect_timeout,
            self.connect_throttle_timeout
        )
    }
}

#[derive(Clone, Default)]
#[non_exhaustive]
pub struct TlsOptions {
    pub danger_accept_invalid_certs: Option<bool>,

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub ca_certificates: Option<Vec<CertificateDer<'static>>>,

    #[cfg(feature = "native-tls")]
    pub ca_certificates: Option<Vec<tokio_native_tls::native_tls::Certificate>>,

    #[cfg(feature = "native-tls")]
    pub danger_accept_invalid_hostnames: Option<bool>,
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
    pub fn add_ca_certificate(mut self, cert: CertificateDer<'static>) -> Self {
        self.ca_certificates.get_or_insert_with(Vec::new).push(cert);
        self
    }

    #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
    pub fn add_ca_certificates<T: IntoIterator<Item = CertificateDer<'static>>>(
        mut self,
        certs: T,
    ) -> Self {
        self.ca_certificates
            .get_or_insert_with(Vec::new)
            .extend(certs);
        self
    }

    #[cfg(feature = "native-tls")]
    pub fn add_ca_certificate(mut self, cert: tokio_native_tls::native_tls::Certificate) -> Self {
        self.ca_certificates.get_or_insert_with(Vec::new).push(cert);
        self
    }

    #[cfg(feature = "native-tls")]
    pub fn add_ca_certificates<
        T: IntoIterator<Item = tokio_native_tls::native_tls::Certificate>,
    >(
        mut self,
        certs: T,
    ) -> Self {
        self.ca_certificates
            .get_or_insert_with(Vec::new)
            .extend(certs);
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
        let store = match self.ca_certificates {
            Some(certs) if certs.is_empty() => {
                return Err(error::Error::invalid_argument(
                    "ca_certificates",
                    "ca_certificates list was provided but is empty",
                ));
            }
            Some(certs) => {
                let mut store = RootCertStore::empty();
                for cert in certs {
                    store.add(cert).map_err(|e| {
                        error::Error::other_failure(format!("failed to add cert: {e}"))
                    })?;
                }
                store
            }
            None => {
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
            }
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
            _ => builder.with_no_client_auth(),
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

        match self.ca_certificates {
            Some(certs) if certs.is_empty() => {
                return Err(error::Error::invalid_argument(
                    "ca_certificates",
                    "ca_certificates list was provided but is empty",
                ));
            }
            Some(certs) => {
                for cert in certs {
                    builder.add_root_certificate(cert);
                }
            }
            None => {
                debug!("Adding Capella root CA to trust store");
                let capella_ca =
                    tokio_native_tls::native_tls::Certificate::from_pem(CAPELLA_CERT.as_ref())
                        .map_err(|e| {
                            error::Error::other_failure(format!("failed to add capella cert: {e}"))
                        })?;
                builder.add_root_certificate(capella_ca);
            }
        }

        match auth {
            Authenticator::CertificateAuthenticator(a) => {
                builder.identity(a.identity.clone());
            }
            Authenticator::PasswordAuthenticator(_) => {}
            Authenticator::JwtAuthenticator(_) => {}
        };

        builder
            .build()
            .map_err(|e| error::Error::other_failure(format!("failed to build client config: {e}")))
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl Display for TlsOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "rustls-tls")
    }
}

#[cfg(feature = "native-tls")]
impl Display for TlsOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "native-tls")
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
#[cfg(feature = "unstable-dns-options")]
pub struct DnsOptions {
    pub namespace: SocketAddr,
    pub timeout: Option<Duration>,
}
#[cfg(feature = "unstable-dns-options")]
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
#[cfg(feature = "unstable-dns-options")]
impl From<DnsOptions> for couchbase_connstr::DnsConfig {
    fn from(opts: DnsOptions) -> Self {
        couchbase_connstr::DnsConfig {
            namespace: opts.namespace,
            timeout: opts.timeout,
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct OrphanReporterOptions {
    pub enabled: Option<bool>,
    pub reporter_interval: Option<Duration>,
    pub sample_size: Option<usize>,
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

impl Display for OrphanReporterOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ enabled: {:?}, reporter_interval: {:?}, sample_size: {:?} }}",
            self.enabled, self.reporter_interval, self.sample_size
        )
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

impl Display for ClusterOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "{{ authenticator: {}, compression_mode: {:?}, tls_options: {}, tcp_keep_alive_time: {:?}, poller_options: {}, http_options: {}, kv_options: {}, orphan_reporter_options: {} }}",
            self.authenticator,
            self.compression_mode,
            if let Some(tls) = &self.tls_options {tls.to_string()} else {"none".to_string()},
            self.tcp_keep_alive_time,
            self.poller_options,
            self.http_options,
            self.kv_options,
            self.orphan_reporter_options
        )
    }
}
