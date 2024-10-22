use std::io::Write;
use std::sync::{Arc, RwLock};

use couchbase_connstr::ResolvedConnSpec;
use envconfig::Envconfig;
use log::LevelFilter;

pub static TEST_CONFIG: RwLock<Option<Arc<TestConfig>>> = RwLock::new(None);

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
    pub conn_str: String,
    pub default_bucket: String,
    pub default_scope: String,
    pub default_collection: String,
    pub data_timeout: String,
    pub resolved_conn_spec: ResolvedConnSpec,
}

pub fn setup_tests() {
    let mut config = TEST_CONFIG.write().unwrap();

    if config.is_none() {
        env_logger::Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} {} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                    record.level(),
                    record.args()
                )
            })
            .filter_level(LevelFilter::Trace)
            .init();
        let test_config = EnvTestConfig::init_from_env().unwrap();

        let conn_spec = couchbase_connstr::parse(&test_config.conn_string).unwrap();

        *config = Some(Arc::new(TestConfig {
            username: test_config.username,
            password: test_config.password,
            conn_str: test_config.conn_string,
            default_bucket: test_config.default_bucket,
            default_scope: test_config.default_scope,
            default_collection: test_config.default_collection,
            data_timeout: test_config.data_timeout,
            resolved_conn_spec: couchbase_connstr::resolve(conn_spec).unwrap(),
        }));
    }
}
pub fn test_username() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.username.clone()
}

pub fn test_password() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.password.clone()
}

pub fn test_conn_str() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.conn_str.clone()
}

pub fn test_bucket() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.default_bucket.clone()
}

pub fn test_scope() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.default_scope.clone()
}

pub fn test_collection() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.default_collection.clone()
}

pub fn test_data_timeout() -> String {
    let guard = TEST_CONFIG.read().unwrap();
    let config = guard.clone().unwrap();
    config.data_timeout.clone()
}
