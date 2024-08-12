use typed_builder::TypedBuilder;

use crate::authenticator::Authenticator;

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct AgentOptions {
    pub tls_config: Option<TlsConfig>,
    pub authenticator: Option<Authenticator>,
    pub bucket_name: Option<String>,

    pub seed_config: SeedConfig,
    pub compression_config: CompressionConfig,
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
pub enum TlsConfig {}
