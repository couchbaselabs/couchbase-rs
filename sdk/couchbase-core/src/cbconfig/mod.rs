pub mod parse;

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct VBucketServerMap {
    #[serde(alias = "hashAlgorithm")]
    pub hash_algorithm: String,
    #[serde(alias = "numReplicas")]
    pub num_replicas: usize,
    #[serde(alias = "serverList")]
    pub server_list: Vec<String>,
    #[serde(alias = "vBucketMap")]
    pub vbucket_map: Vec<Vec<i16>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigDDocs {
    #[serde(alias = "uri")]
    pub uri: String,
}

#[derive(Deserialize, Debug, Default, Clone)]
pub struct TerseExtNodePorts {
    #[serde(alias = "kv")]
    pub kv: Option<i64>,
    #[serde(alias = "capi")]
    pub capi: Option<i64>,
    #[serde(alias = "mgmt")]
    pub mgmt: i64,
    #[serde(alias = "n1ql")]
    pub n1ql: Option<i64>,
    #[serde(alias = "fts")]
    pub fts: Option<i64>,
    #[serde(alias = "cbas")]
    pub cbas: Option<i64>,
    #[serde(alias = "eventingAdminPort")]
    pub eventing: Option<i64>,
    #[serde(alias = "indexHttp")]
    pub gsi: Option<i64>,
    #[serde(alias = "backupAPI")]
    pub backup: Option<i64>,

    #[serde(alias = "kvSSL")]
    pub kv_ssl: Option<i64>,
    #[serde(alias = "capiSSL")]
    pub capi_ssl: Option<i64>,
    #[serde(alias = "mgmtSSL")]
    pub mgmt_ssl: i64,
    #[serde(alias = "n1qlSSL")]
    pub n1ql_ssl: Option<i64>,
    #[serde(alias = "ftsSSL")]
    pub fts_ssl: Option<i64>,
    #[serde(alias = "cbasSSL")]
    pub cbas_ssl: Option<i64>,
    #[serde(alias = "eventingSSL")]
    pub eventing_ssl: Option<i64>,
    #[serde(alias = "indexHttps")]
    pub gsi_ssl: Option<i64>,
    #[serde(alias = "backupAPIHTTPS")]
    pub backup_ssl: Option<i64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TerseExtNodeAltAddresses {
    #[serde(alias = "ports")]
    pub ports: TerseExtNodePorts,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TerseNodePorts {
    #[serde(alias = "direct")]
    pub direct: Option<u16>,
    #[serde(alias = "proxy")]
    pub proxy: Option<u16>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TerseNodeConfig {
    #[serde(alias = "couchbaseApiBase")]
    pub couchbase_api_base: Option<String>,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
    #[serde(alias = "ports")]
    pub ports: Option<TerseNodePorts>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TerseNodeExtConfig {
    #[serde(alias = "services")]
    pub services: Option<TerseExtNodePorts>,
    #[serde(alias = "thisNode")]
    pub this_node: Option<bool>,
    #[serde(alias = "hostname")]
    pub hostname: Option<String>,
    #[serde(alias = "alternateAddresses", default)]
    pub alternate_addresses: HashMap<String, TerseExtNodeAltAddresses>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TerseConfig {
    #[serde(alias = "rev")]
    pub rev: i64,
    #[serde(alias = "revEpoch")]
    pub rev_epoch: Option<i64>,
    #[serde(alias = "name")]
    pub name: Option<String>,
    #[serde(alias = "nodeLocator")]
    pub node_locator: Option<String>,
    #[serde(alias = "uuid")]
    pub uuid: Option<String>,
    #[serde(alias = "uri")]
    pub uri: Option<String>,
    #[serde(alias = "streamingUri")]
    pub streaming_uri: Option<String>,
    #[serde(alias = "bucketCapabilitiesVer")]
    pub bucket_capabilities_ver: Option<String>,
    #[serde(alias = "bucketCapabilities")]
    pub bucket_capabilities: Option<Vec<String>>,
    #[serde(alias = "collectionsManifestUid")]
    pub collections_manifest_uuid: Option<String>,
    #[serde(alias = "ddocs")]
    pub ddocs: Option<ConfigDDocs>,
    #[serde(alias = "vBucketServerMap")]
    pub vbucket_server_map: Option<VBucketServerMap>,
    #[serde(alias = "nodes")]
    pub nodes: Option<Vec<TerseNodeConfig>>,
    #[serde(alias = "nodesExt")]
    pub nodes_ext: Vec<TerseNodeExtConfig>,
    #[serde(alias = "clusterCapabilitiesVer")]
    pub cluster_capabilities_ver: Vec<i64>,
    #[serde(alias = "clusterCapabilities")]
    pub cluster_capabilities: HashMap<String, Vec<String>>,
    #[serde(alias = "clusterUUID")]
    pub cluster_uuid: Option<String>,
    #[serde(alias = "clusterName")]
    pub cluster_name: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifestCollection {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "maxTTL", default)]
    pub max_ttl: Option<i32>,
    #[serde(rename = "history", default)]
    pub history: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifestScope {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "collections", default)]
    pub collections: Vec<CollectionManifestCollection>,
}

#[derive(Deserialize, Debug, Clone)]
#[non_exhaustive]
pub struct CollectionManifest {
    #[serde(rename = "uid")]
    pub uid: String,
    #[serde(rename = "scopes", default)]
    pub scopes: Vec<CollectionManifestScope>,
}
