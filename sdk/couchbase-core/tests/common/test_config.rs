use std::io::Write;
use std::sync::Arc;

use envconfig::Envconfig;
use futures::executor::block_on;
use lazy_static::lazy_static;
use log::{trace, LevelFilter};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;

lazy_static! {
    pub static ref TEST_CONFIG: Mutex<Option<Arc<TestConfig>>> = Mutex::new(None);
}

#[derive(Debug, Clone, Envconfig)]
pub struct EnvTestConfig {
    #[envconfig(from = "RCBUSERNAME", default = "Administrator")]
    pub username: String,
    #[envconfig(from = "RCBPASSWORD", default = "password")]
    pub password: String,
    #[envconfig(from = "RCBCONNSTR", default = "couchbases://192.168.107.128")]
    pub conn_string: String,
    #[envconfig(from = "RCBBUCKET", default = "default")]
    pub default_bucket: String,
    #[envconfig(from = "RCBSCOPE", default = "_default")]
    pub default_scope: String,
    #[envconfig(from = "RCBCOLLECTION", default = "_default")]
    pub default_collection: String,
    #[envconfig(from = "RCBDATA_TIMEOUT", default = "2500")]
    pub data_timeout: String,
}

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub username: String,
    pub password: String,
    pub memd_addrs: Vec<String>,
    pub default_bucket: String,
    pub default_scope: String,
    pub default_collection: String,
    pub data_timeout: String,
    pub use_ssl: bool,
}

pub async fn setup_tests() {
    let mut config = TEST_CONFIG.lock().await;

    if config.is_none() {
        env_logger::Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} {} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                    record.level(),
                    record.args()
                )
            })
            .filter(Some("rustls"), LevelFilter::Warn)
            .filter_level(LevelFilter::Trace)
            .init();
        let test_config = EnvTestConfig::init_from_env().unwrap();

        // TODO: Once we have connection string parsing in place this should go away.
        let conn_spec = couchbase_connstr::parse(test_config.conn_string).unwrap();
        let resolved = couchbase_connstr::resolve(conn_spec).await.unwrap();

        *config = Some(Arc::new(TestConfig {
            username: test_config.username,
            password: test_config.password,
            memd_addrs: resolved.memd_hosts.iter().map(|h| h.to_string()).collect(),
            default_bucket: test_config.default_bucket,
            default_scope: test_config.default_scope,
            default_collection: test_config.default_collection,
            data_timeout: test_config.data_timeout,
            use_ssl: resolved.use_ssl,
        }));

        trace!("{:?}", &config);
    }
}

pub async fn test_username() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.username.clone()
}

pub async fn test_password() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.password.clone()
}

pub async fn test_mem_addrs() -> Vec<String> {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.memd_addrs.clone()
}

pub async fn test_bucket() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.default_bucket.clone()
}

pub async fn test_scope() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.default_scope.clone()
}

pub async fn test_collection() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.default_collection.clone()
}

pub async fn test_data_timeout() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.data_timeout.clone()
}

pub async fn test_is_ssl() -> bool {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.use_ssl
}
