#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use std::sync::Arc;
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
pub type TlsConfig = Arc<tokio_rustls::rustls::ClientConfig>;

#[cfg(feature = "native-tls")]
pub type TlsConfig = tokio_native_tls::native_tls::TlsConnector;

#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
compile_error!("At least one of the features 'rustls-tls' or 'native-tls' must be enabled.");
