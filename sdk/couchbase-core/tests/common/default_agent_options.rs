use crate::common::test_config::TEST_CONFIG;
use couchbase_core::agentoptions::{AgentOptions, CompressionConfig, SeedConfig};
use couchbase_core::authenticator::{Authenticator, PasswordAuthenticator};
#[cfg(feature = "rustls-tls")]
use {
    couchbase_core::insecure_certverfier::InsecureCertVerifier, std::sync::Arc,
    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider,
    tokio_rustls::rustls::crypto::CryptoProvider,
};

pub fn create_default_options() -> AgentOptions {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    drop(guard);

    let tls_config = if config.use_ssl {
        #[cfg(feature = "native-tls")]
        {
            let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();
            builder.danger_accept_invalid_certs(true);
            Some(builder.build().unwrap())
        }
        #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
        Some(get_rustls_config())
    } else {
        None
    };

    AgentOptions::builder()
        .tls_config(tls_config)
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

    let tls_config = if config.use_ssl {
        #[cfg(feature = "native-tls")]
        {
            let mut builder = tokio_native_tls::native_tls::TlsConnector::builder();
            builder.danger_accept_invalid_certs(true);
            Some(builder.build().unwrap())
        }
        #[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
        Some(get_rustls_config())
    } else {
        None
    };

    AgentOptions::builder()
        .tls_config(tls_config)
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

#[cfg(feature = "rustls-tls")]
fn get_rustls_config() -> Arc<tokio_rustls::rustls::ClientConfig> {
    let _ = CryptoProvider::install_default(default_provider());
    Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier {}))
            .with_no_client_auth(),
    )
}
