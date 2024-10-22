use crate::common::test_config::TEST_CONFIG;
use couchbase::options::cluster_options::{ClusterOptions, TlsOptions};
use couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};

pub fn create_default_options() -> ClusterOptions {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    drop(guard);

    let tls_options = if config.resolved_conn_spec.use_ssl {
        Some(
            TlsOptions::builder()
                .danger_accept_invalid_certs(true)
                .build(),
        )
    } else {
        None
    };

    ClusterOptions::builder()
        .tls_options(tls_options)
        .authenticator(Authenticator::PasswordAuthenticator(
            PasswordAuthenticator {
                username: config.username.clone(),
                password: config.password.clone(),
            },
        ))
        .build()
}
