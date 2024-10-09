use crate::common::test_config::TEST_CONFIG;
use couchbase::options::cluster_options::ClusterOptions;
use couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};

pub fn create_default_options() -> ClusterOptions {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    drop(guard);

    ClusterOptions::builder()
        .authenticator(Authenticator::PasswordAuthenticator(
            PasswordAuthenticator {
                username: config.username.clone(),
                password: config.password.clone(),
            },
        ))
        .build()
}
