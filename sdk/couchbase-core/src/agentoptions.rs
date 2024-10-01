use std::time::Duration;

use typed_builder::TypedBuilder;

use crate::authenticator::Authenticator;
use crate::memdx::connection::TlsConfig as MemdxTlsConfig;

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct AgentOptions {
    #[builder(default)]
    pub tls_config: Option<TlsConfig>,
    pub authenticator: Authenticator,
    #[builder(default)]
    pub bucket_name: Option<String>,

    #[builder(default)]
    pub connect_timeout: Option<Duration>,
    #[builder(default)]
    pub connect_throttle_timeout: Option<Duration>,

    #[builder(default)]
    pub seed_config: SeedConfig,
    #[builder(default)]
    pub compression_config: CompressionConfig,
    #[builder(default)]
    pub config_poller_config: ConfigPollerConfig,
}

#[derive(Default, Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SeedConfig {
    pub http_addrs: Vec<String>,
    pub memd_addrs: Vec<String>,
}

#[derive(Default, Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct CompressionConfig {
    pub disable_decompression: bool,
    pub mode: CompressionMode,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CompressionMode {
    Enabled { min_size: usize, min_ratio: f64 },
    Disabled,
}

impl Default for CompressionMode {
    fn default() -> Self {
        Self::Enabled {
            min_size: 32,
            min_ratio: 0.83,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ConfigPollerConfig {
    pub poll_interval: Duration,
    pub floor_interval: Duration,
}

impl Default for ConfigPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(2500),
            floor_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum TlsConfig {
    NoVerify,
}

impl From<TlsConfig> for MemdxTlsConfig {
    fn from(value: TlsConfig) -> Self {
        MemdxTlsConfig {
            root_certs: None,
            accept_all_certs: Some(true),
        }
    }
}
