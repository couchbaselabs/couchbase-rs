use serde::Deserialize;
use std::fmt;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub struct Index {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub using: String,
    #[serde(default)]
    pub state: IndexState,
    pub is_primary: Option<bool>,
    pub keyspace_id: Option<String>,
    pub namespace_id: Option<String>,
    pub index_key: Option<Vec<String>>,
    pub condition: Option<String>,
    pub partition: Option<String>,
    pub scope_id: Option<String>,
    pub bucket_id: Option<String>,
}

#[derive(Debug, Eq, PartialEq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexState {
    Deferred,
    Building,
    Pending,
    Online,
    Offline,
    Abridged,
    Scheduled,
    #[serde(other)]
    #[default]
    Unknown,
}

impl fmt::Display for IndexState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_str = match self {
            IndexState::Unknown => "Unknown",
            IndexState::Deferred => "Deferred",
            IndexState::Building => "Building",
            IndexState::Pending => "Pending",
            IndexState::Online => "Online",
            IndexState::Offline => "Offline",
            IndexState::Abridged => "Abridged",
            IndexState::Scheduled => "Scheduled",
        };
        write!(f, "{state_str}")
    }
}
