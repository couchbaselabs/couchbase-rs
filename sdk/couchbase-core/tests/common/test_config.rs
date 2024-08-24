use std::io::Write;
use std::sync::{Arc, Mutex};

use envconfig::Envconfig;
use log::LevelFilter;

pub static TEST_CONFIG: Mutex<Option<Arc<TestConfig>>> = Mutex::new(None);

#[derive(Debug, Clone, Envconfig)]
pub struct EnvTestConfig {
    #[envconfig(from = "RCBUSERNAME", default = "Administrator")]
    pub username: String,
    #[envconfig(from = "RCBPASSWORD", default = "password")]
    pub password: String,
    #[envconfig(from = "RCBCONNSTR", default = "192.168.107.128:11210")]
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
}

pub fn setup_tests() {
    let mut config = TEST_CONFIG.lock().unwrap();

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

        // TODO: Once we have connection string parsing in place this should go away.
        let mut conn_string = test_config.conn_string.replace("couchbases://", "");
        conn_string = conn_string.replace("couchbase://", "");

        let (conn_string, port) = if let Some(pos) = conn_string.find(":") {
            let split = conn_string.split_at(pos);
            let port_str = split.1;
            let port_str = port_str.replace(":", "");
            (split.0.to_string(), port_str.parse().unwrap())
        } else {
            (conn_string, 11210)
        };

        let memd_addrs = conn_string
            .split(',')
            .map(|addr| format!("{}:{}", addr, port))
            .collect();

        *config = Some(Arc::new(TestConfig {
            username: test_config.username,
            password: test_config.password,
            memd_addrs,
            default_bucket: test_config.default_bucket,
            data_timeout: test_config.data_timeout,
        }));
    }
}
pub fn test_username() -> String {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    config.username.clone()
}

pub fn test_password() -> String {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    config.password.clone()
}

pub fn test_mem_addrs() -> Vec<String> {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    config.memd_addrs.clone()
}

pub fn test_bucket() -> String {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    config.default_bucket.clone()
}

pub fn test_data_timeout() -> String {
    let guard = TEST_CONFIG.lock().unwrap();
    let config = guard.clone().unwrap();
    config.data_timeout.clone()
}
