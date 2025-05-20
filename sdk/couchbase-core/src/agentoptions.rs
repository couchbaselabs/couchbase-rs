use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::tls_config::TlsConfig;
use std::time::Duration;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AgentOptions {
    pub(crate) seed_config: SeedConfig,
    pub(crate) authenticator: Authenticator,

    pub(crate) auth_mechanisms: Vec<AuthMechanism>,
    pub(crate) tls_config: Option<TlsConfig>,
    pub(crate) bucket_name: Option<String>,

    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) connect_throttle_timeout: Option<Duration>,

    pub(crate) compression_config: CompressionConfig,
    pub(crate) config_poller_config: ConfigPollerConfig,
}

impl AgentOptions {
    pub fn new(seed_config: SeedConfig, authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
            bucket_name: None,
            connect_timeout: None,
            connect_throttle_timeout: None,
            seed_config,
            compression_config: CompressionConfig::default(),
            config_poller_config: ConfigPollerConfig::default(),
            auth_mechanisms: vec![],
        }
    }

    pub fn seed_config(mut self, seed_config: SeedConfig) -> Self {
        self.seed_config = seed_config;
        self
    }

    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = authenticator;
        self
    }

    pub fn tls_config(mut self, tls_config: impl Into<Option<TlsConfig>>) -> Self {
        self.tls_config = tls_config.into();
        self
    }

    pub fn bucket_name(mut self, bucket_name: impl Into<Option<String>>) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn connect_timeout(mut self, connect_timeout: impl Into<Option<Duration>>) -> Self {
        self.connect_timeout = connect_timeout.into();
        self
    }

    pub fn connect_throttle_timeout(
        mut self,
        connect_throttle_timeout: impl Into<Option<Duration>>,
    ) -> Self {
        self.connect_throttle_timeout = connect_throttle_timeout.into();
        self
    }

    pub fn compression_config(mut self, compression_config: CompressionConfig) -> Self {
        self.compression_config = compression_config;
        self
    }

    pub fn config_poller_config(mut self, config_poller_config: ConfigPollerConfig) -> Self {
        self.config_poller_config = config_poller_config;
        self
    }

    pub fn auth_mechanisms(mut self, auth_mechanisms: Vec<AuthMechanism>) -> Self {
        self.auth_mechanisms = auth_mechanisms;
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeedConfig {
    pub(crate) http_addrs: Vec<String>,
    pub(crate) memd_addrs: Vec<String>,
}

impl SeedConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn http_addrs(mut self, http_addrs: Vec<String>) -> Self {
        self.http_addrs = http_addrs;
        self
    }

    pub fn memd_addrs(mut self, memd_addrs: Vec<String>) -> Self {
        self.memd_addrs = memd_addrs;
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CompressionConfig {
    pub(crate) disable_decompression: bool,
    pub(crate) mode: CompressionMode,
}

impl CompressionConfig {
    pub fn new(mode: CompressionMode) -> Self {
        Self {
            disable_decompression: false,
            mode,
        }
    }

    pub fn disable_decompression(mut self, disable_decompression: bool) -> Self {
        self.disable_decompression = disable_decompression;
        self
    }
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
    pub(crate) poll_interval: Duration,
    pub(crate) floor_interval: Duration,
}

impl ConfigPollerConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn floor_interval(mut self, floor_interval: Duration) -> Self {
        self.floor_interval = floor_interval;
        self
    }
}

impl Default for ConfigPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(2500),
            floor_interval: Duration::from_millis(50),
        }
    }
}
