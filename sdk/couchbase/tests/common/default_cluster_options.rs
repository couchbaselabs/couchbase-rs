use crate::common::test_config::TEST_CONFIG;
use couchbase::options::cluster_options::{ClusterOptions, TlsOptions};
use couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};

pub async fn create_default_options() -> ClusterOptions {
    let guard = TEST_CONFIG.read().await;
    let config = guard.clone().unwrap();
    drop(guard);

    let mut opts = ClusterOptions::new(Authenticator::PasswordAuthenticator(
        PasswordAuthenticator {
            username: config.username.clone(),
            password: config.password.clone(),
        },
    ));

    if config.resolved_conn_spec.use_ssl {
        opts = opts.tls_options(TlsOptions::new().danger_accept_invalid_certs(true));
    };

    opts
}
