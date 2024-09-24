use rscbx_couchbase_core::agentoptions::{AgentOptions, CompressionConfig, SeedConfig};
use rscbx_couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};

use crate::common::test_config::TEST_CONFIG;

pub fn create_default_options() -> AgentOptions {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    drop(guard);

    AgentOptions::builder()
        .tls_config(None)
        .authenticator(Authenticator::PasswordAuthenticator(
            PasswordAuthenticator {
                username: config.username.clone(),
                password: config.password.clone(),
            },
        ))
        .bucket_name(config.default_bucket.clone())
        .seed_config(
            SeedConfig::builder()
                .memd_addrs(config.memd_addrs.clone())
                .build(),
        )
        .compression_config(CompressionConfig::default())
        .build()
}

pub fn create_options_without_bucket() -> AgentOptions {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    drop(guard);

    AgentOptions::builder()
        .tls_config(None)
        .authenticator(Authenticator::PasswordAuthenticator(
            PasswordAuthenticator {
                username: config.username.clone(),
                password: config.password.clone(),
            },
        ))
        .seed_config(
            SeedConfig::builder()
                .memd_addrs(config.memd_addrs.clone())
                .build(),
        )
        .compression_config(CompressionConfig::default())
        .build()
}
