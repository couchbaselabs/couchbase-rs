use serde_derive::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Copy, Clone, Deserialize)]
pub enum ClusterType {
    #[serde(rename(deserialize = "standalone"))]
    Standalone,
    #[serde(rename(deserialize = "mock"))]
    Mock,
}

// TODO: support disabling individual tests and also support feature flags
#[derive(Debug, Deserialize)]
pub struct FileConfig {
    #[serde(rename(deserialize = "type"))]
    cluster_type: ClusterType,
    #[serde(rename(deserialize = "standalone"))]
    standalone_config: Option<StandaloneConfig>,
    #[serde(rename(deserialize = "mock"))]
    caves_config: Option<CavesConfig>,
    tests: Option<String>,
}

pub struct Config {
    cluster_type: ClusterType,
    standalone_config: Option<StandaloneConfig>,
    caves_config: Option<CavesConfig>,
    enabled_tests: Vec<String>,
}

impl Config {
    pub fn cluster_type(&self) -> ClusterType {
        self.cluster_type
    }
    pub fn standalone_config(&self) -> Option<StandaloneConfig> {
        self.standalone_config.clone()
    }
    pub fn mock_config(&self) -> Option<CavesConfig> {
        self.caves_config.clone()
    }
    pub fn tests(&self) -> Vec<String> {
        self.enabled_tests.clone()
    }

    pub fn try_load_config() -> Option<Config> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("integration");
        path.push("config.toml");
        let config: FileConfig = match fs::read_to_string(&path) {
            Ok(r) => match toml::from_str(&r) {
                Ok(i) => Some(i),
                Err(e) => {
                    panic!("Failed to parse config file: {}", e);
                }
            },
            Err(_e) => None,
        }?;

        let enabled_tests = match config.tests {
            Some(ref t) => t
                .clone()
                .split(",")
                .map(|i| i.to_string())
                .collect::<Vec<String>>(),
            None => Vec::new(),
        };

        Some(Config {
            enabled_tests,
            standalone_config: config.standalone_config,
            cluster_type: config.cluster_type,
            caves_config: config.caves_config,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StandaloneConfig {
    username: String,
    password: String,
    #[serde(alias = "conn-string")]
    conn_string: String,
    #[serde(alias = "default-bucket")]
    default_bucket: Option<String>,
    #[serde(alias = "default-scope")]
    default_scope: Option<String>,
    #[serde(alias = "default-collection")]
    default_collection: Option<String>,
}

impl StandaloneConfig {
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
    pub fn conn_string(&self) -> &str {
        &self.conn_string
    }
    pub fn default_bucket(&self) -> Option<String> {
        self.default_bucket.clone()
    }
    pub fn default_scope(&self) -> Option<String> {
        self.default_scope.clone()
    }
    pub fn default_collection(&self) -> Option<String> {
        self.default_collection.clone()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CavesConfig {
    version: String,
}

impl CavesConfig {
    pub fn version(&self) -> String {
        self.version.clone()
    }
}
