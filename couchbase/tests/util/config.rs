use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Copy, Clone, Deserialize)]
pub(crate) enum ClusterType {
    #[serde(rename(deserialize = "standalone"))]
    Standalone,
    #[serde(rename(deserialize = "mock"))]
    Mock,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    #[serde(rename(deserialize = "type"))]
    cluster_type: ClusterType,
    #[serde(rename(deserialize = "standalone"))]
    standalone_config: Option<StandaloneConfig>,
    #[serde(rename(deserialize = "mock"))]
    caves_config: Option<CavesConfig>,
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

    pub fn try_load_config() -> Option<Config> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("config");
        match fs::read_to_string(&path) {
            Ok(r) => match toml::from_str(&r) {
                Ok(i) => Some(i),
                Err(e) => {
                    panic!("Failed to parse config file: {}", e);
                }
            },
            Err(_e) => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StandaloneConfig {
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
pub(crate) struct CavesConfig {
    version: String,
}

impl CavesConfig {
    pub fn version(&self) -> String {
        self.version.clone()
    }
}
