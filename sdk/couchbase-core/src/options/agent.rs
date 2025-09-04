use couchbase_connstr::Address;

use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::tls_config::TlsConfig;
use std::time::Duration;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AgentOptions {
    pub seed_config: SeedConfig,
    pub authenticator: Authenticator,

    pub auth_mechanisms: Vec<AuthMechanism>,
    pub tls_config: Option<TlsConfig>,
    pub bucket_name: Option<String>,

    pub compression_config: CompressionConfig,
    pub config_poller_config: ConfigPollerConfig,
    pub kv_config: KvConfig,
    pub http_config: HttpConfig,
    pub tcp_keep_alive_time: Option<Duration>,
}

impl AgentOptions {
    pub fn new(seed_config: SeedConfig, authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
            bucket_name: None,
            seed_config,
            compression_config: CompressionConfig::default(),
            config_poller_config: ConfigPollerConfig::default(),
            auth_mechanisms: vec![],
            kv_config: KvConfig::default(),
            http_config: HttpConfig::default(),
            tcp_keep_alive_time: None,
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

    pub fn kv_config(mut self, kv_config: KvConfig) -> Self {
        self.kv_config = kv_config;
        self
    }

    pub fn http_config(mut self, http_config: HttpConfig) -> Self {
        self.http_config = http_config;
        self
    }

    pub fn tcp_keep_alive_time(mut self, tcp_keep_alive: Duration) -> Self {
        self.tcp_keep_alive_time = Some(tcp_keep_alive);
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeedConfig {
    pub http_addrs: Vec<Address>,
    pub memd_addrs: Vec<Address>,
}

impl SeedConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn http_addrs(mut self, http_addrs: Vec<Address>) -> Self {
        self.http_addrs = http_addrs;
        self
    }

    pub fn memd_addrs(mut self, memd_addrs: Vec<Address>) -> Self {
        self.memd_addrs = memd_addrs;
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CompressionConfig {
    pub disable_decompression: bool,
    pub mode: CompressionMode,
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

    pub fn mode(mut self, mode: CompressionMode) -> Self {
        self.mode = mode;
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
    pub poll_interval: Duration,
}

impl ConfigPollerConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }
}

impl Default for ConfigPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(2500),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct KvConfig {
    pub enable_mutation_tokens: bool,
    pub enable_server_durations: bool,
    pub num_connections: usize,
    pub connect_timeout: Duration,
    pub connect_throttle_timeout: Duration,
}

impl KvConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable_mutation_tokens(mut self, enable: bool) -> Self {
        self.enable_mutation_tokens = enable;
        self
    }

    pub fn enable_server_durations(mut self, enable: bool) -> Self {
        self.enable_server_durations = enable;
        self
    }

    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub fn connect_throttle_timeout(mut self, connect_throttle_timeout: Duration) -> Self {
        self.connect_throttle_timeout = connect_throttle_timeout;
        self
    }

    pub fn num_connections(mut self, num: usize) -> Self {
        self.num_connections = num;
        self
    }
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            enable_mutation_tokens: true,
            enable_server_durations: true,
            num_connections: 1,
            connect_timeout: Duration::from_secs(10),
            connect_throttle_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct HttpConfig {
    pub max_idle_connections_per_host: Option<usize>,
    pub idle_connection_timeout: Duration,
}

impl HttpConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_idle_connections_per_host(mut self, max_idle_connections_per_host: usize) -> Self {
        self.max_idle_connections_per_host = Some(max_idle_connections_per_host);
        self
    }

    pub fn idle_connection_timeout(mut self, idle_connection_timeout: Duration) -> Self {
        self.idle_connection_timeout = idle_connection_timeout;
        self
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_idle_connections_per_host: None,
            idle_connection_timeout: Duration::from_secs(1),
        }
    }
}
