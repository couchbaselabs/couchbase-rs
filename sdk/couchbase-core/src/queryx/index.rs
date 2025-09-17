use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub struct Index {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub using: String,
    #[serde(default)]
    pub state: String,
    pub is_primary: Option<bool>,
    pub keyspace_id: Option<String>,
    pub namespace_id: Option<String>,
    pub index_key: Option<Vec<String>>,
    pub condition: Option<String>,
    pub partition: Option<String>,
    pub scope_id: Option<String>,
    pub bucket_id: Option<String>,
}
