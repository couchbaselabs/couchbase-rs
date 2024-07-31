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
    // pub compression_config: Option<CompressionConfig>,
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
    pub min_size: Option<i32>,
    pub min_ratio: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TlsConfig {}
