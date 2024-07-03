use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct VBucketServerMap {
    #[serde(alias = "hashAlgorithm")]
    hash_algorithm: String,
    #[serde(alias = "numReplicas")]
    num_replicas: i8,
    #[serde(alias = "serverList")]
    server_list: Vec<String>,
    #[serde(alias = "vBucketMap")]
    vbucket_map: Vec<Vec<i8>>,
}

#[derive(Deserialize, Debug)]
pub struct ConfigDDocs {
    #[serde(alias = "uri")]
    uri: String,
}

#[derive(Deserialize, Debug)]
pub struct TerseExtNodePorts {
    #[serde(alias = "kv")]
    kv: Option<i16>,
    #[serde(alias = "capi")]
    capi: Option<i16>,
    #[serde(alias = "mgmt")]
    mgmt: Option<i16>,
    #[serde(alias = "n1ql")]
    n1ql: Option<i16>,
    #[serde(alias = "fts")]
    fts: Option<i16>,
    #[serde(alias = "cbas")]
    cbas: Option<i16>,
    #[serde(alias = "eventingAdminPort")]
    eventing: Option<i16>,
    #[serde(alias = "indexHttp")]
    gsi: Option<i16>,
    #[serde(alias = "backupAPI")]
    backup: Option<i16>,

    #[serde(alias = "kvSSL")]
    kv_ssl: Option<i16>,
    #[serde(alias = "capiSSL")]
    capi_ssl: Option<i16>,
    #[serde(alias = "mgmtSSL")]
    mgmt_ssl: Option<i16>,
    #[serde(alias = "n1qlSSL")]
    n1ql_ssl: Option<i16>,
    #[serde(alias = "ftsSSL")]
    fts_ssl: Option<i16>,
    #[serde(alias = "cbasSSL")]
    cbas_ssl: Option<i16>,
    #[serde(alias = "eventingSSL")]
    eventing_ssl: Option<i16>,
    #[serde(alias = "indexHttps")]
    gsi_ssl: Option<i16>,
    #[serde(alias = "backupAPIHTTPS")]
    backup_ssl: Option<i16>,
}

#[derive(Deserialize, Debug)]
pub struct TerseExtNodeAltAddresses {
    #[serde(alias = "ports")]
    ports: Option<TerseExtNodePorts>,
    #[serde(alias = "hostname")]
    hostname: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct TerseNodePorts {
    #[serde(alias = "direct")]
    direct: Option<u16>,
    #[serde(alias = "proxy")]
    proxy: Option<u16>,
}

#[derive(Deserialize, Debug)]
pub struct TerseNodeConfig {
    #[serde(alias = "couchbaseApiBase")]
    couchbase_api_base: Option<String>,
    #[serde(alias = "hostname")]
    hostname: Option<String>,
    #[serde(alias = "ports")]
    ports: Option<TerseNodePorts>,
}

#[derive(Deserialize, Debug)]
pub struct TerseNodeExtConfig {
    #[serde(alias = "services")]
    services: Option<TerseExtNodePorts>,
    #[serde(alias = "thisNode")]
    this_node: Option<bool>,
    #[serde(alias = "hostname")]
    hostname: Option<String>,
    #[serde(alias = "alternateAddresses")]
    alternate_addresses: Option<TerseExtNodeAltAddresses>,
}

#[derive(Deserialize, Debug)]
pub struct TerseConfig {
    #[serde(alias = "rev")]
    rev: i64,
    #[serde(alias = "revEpoch")]
    rev_epoch: Option<i64>,
    #[serde(alias = "name")]
    name: Option<String>,
    #[serde(alias = "nodeLocator")]
    node_locator: Option<String>,
    #[serde(alias = "uuid")]
    uuid: Option<String>,
    #[serde(alias = "uri")]
    uri: Option<String>,
    #[serde(alias = "streamingUri")]
    streaming_uri: Option<String>,
    #[serde(alias = "bucketCapabilitiesVer")]
    bucket_capabilities_ver: Option<String>,
    #[serde(alias = "bucketCapabilities")]
    bucket_capabilities: Vec<String>,
    #[serde(alias = "collectionsManifestUid")]
    collections_manifest_uuid: Option<String>,
    #[serde(alias = "ddocs")]
    ddocs: Option<ConfigDDocs>,
    #[serde(alias = "vBucketServerMap")]
    vbucket_server_map: Option<VBucketServerMap>,
    #[serde(alias = "nodes")]
    nodes: Vec<TerseNodeConfig>,
    #[serde(alias = "nodes_ext")]
    nodes_ext: Vec<TerseNodeExtConfig>,
    #[serde(alias = "clusterCapabilitiesVer")]
    cluster_capabilities_ver: Vec<i64>,
    #[serde(alias = "clusterCapabilities")]
    cluster_capabilities: HashMap<String, Vec<String>>,
}
