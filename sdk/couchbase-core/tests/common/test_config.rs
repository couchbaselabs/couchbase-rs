use env_logger::fmt::style::Style;
use env_logger::fmt::Formatter;
use env_logger::WriteStyle;
use envconfig::Envconfig;
use futures::executor::block_on;
use lazy_static::lazy_static;
use log::kv::{Error, Key, Source, ToValue, Value, VisitSource};
use log::{debug, kv, trace, LevelFilter};
use std::io::Write;
use std::sync::Arc;
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
    #[envconfig(from = "RCBCONNSTR", default = "couchbase://192.168.107.129")]
    pub conn_string: String,
    #[envconfig(from = "RCBBUCKET", default = "default")]
    pub default_bucket: String,
    #[envconfig(from = "RCBDATA_TIMEOUT", default = "2500")]
    pub data_timeout: String,
}

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub username: String,
    pub password: String,
    pub memd_addrs: Vec<String>,
    pub default_bucket: String,
    pub data_timeout: String,
    pub use_ssl: bool,
}

pub async fn setup_tests() {
    let mut config = TEST_CONFIG.lock().await;

    if config.is_none() {
        env_logger::Builder::new()
            .format(|buf, record| {
                write!(
                    buf,
                    "{}:{} {} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args(),
                )?;
                record
                    .key_values()
                    .visit(&mut DefaultVisitSource(buf))
                    .unwrap();

                writeln!(buf)
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
            data_timeout: test_config.data_timeout,
            use_ssl: resolved.use_ssl,
        }));

        debug!("{:?}", &config);
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

pub async fn test_data_timeout() -> String {
    let guard = TEST_CONFIG.lock().await;
    let config = guard.clone().unwrap();
    config.data_timeout.clone()
}

struct DefaultVisitSource<'a>(&'a mut Formatter);

impl<'a, 'kvs> VisitSource<'kvs> for DefaultVisitSource<'a> {
    fn visit_pair(&mut self, key: Key<'_>, value: Value<'kvs>) -> Result<(), Error> {
        write!(self.0, " {}={}", key, value)?;
        Ok(())
    }
}
