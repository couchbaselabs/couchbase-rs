use std::collections::HashMap;
use std::time::Duration;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use typed_builder::TypedBuilder;

use crate::helpers;
use crate::httpx::request::OnBehalfOfInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
    AtPlus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ProfileMode {
    Off,
    Phases,
    Timings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Compression {
    Zip,
    Rle,
    Lzma,
    Lzo,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum DurabilityLevel {
    None,
    Majority,
    MajorityAndPersistActive,
    PersistToMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum Encoding {
    #[serde(rename = "UTF-8")]
    Utf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Format {
    Json,
    Xml,
    Csv,
    Tsv,
}

#[derive(Debug, Clone, Serialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct CredsJson {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pass: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ReplicaLevel {
    Off,
    On,
}

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct ScanVectorEntry {
    pub seq_no: u64,
    pub vb_uuid: String,
}

impl Serialize for ScanVectorEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.seq_no)?;
        seq.serialize_element(&self.vb_uuid)?;

        seq.end()
    }
}

pub type FullScanVectors = Vec<ScanVectorEntry>;
pub type SparseScanVectors = HashMap<String, ScanVectorEntry>;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct QueryOptions {
    pub args: Option<Vec<Vec<u8>>>,
    pub atr_collection: Option<String>,
    pub auto_execute: Option<bool>,
    pub client_context_id: Option<String>,
    pub compression: Option<Compression>,
    pub controls: Option<bool>,
    pub creds: Option<Vec<CredsJson>>,
    pub durability_level: Option<DurabilityLevel>,
    pub encoded_plan: Option<String>,
    pub encoding: Option<Encoding>,
    pub format: Option<Format>,
    pub kv_timeout: Option<Duration>,
    pub max_parallelism: Option<u32>,
    pub memory_quota: Option<u32>,
    pub metrics: Option<bool>,
    pub namespace: Option<String>,
    pub num_atrs: Option<u32>,
    pub pipeline_batch: Option<u32>,
    pub pipeline_cap: Option<u32>,
    pub prepared: Option<String>,
    pub preserve_expiry: Option<bool>,
    pub pretty: Option<bool>,
    pub profile: Option<ProfileMode>,
    pub query_context: Option<String>,
    pub read_only: Option<bool>,
    pub scan_cap: Option<u32>,
    pub scan_consistency: Option<ScanConsistency>,
    pub sparse_scan_vector: Option<SparseScanVectors>,
    pub full_scan_vector: Option<FullScanVectors>,
    pub sparse_scan_vectors: Option<HashMap<String, SparseScanVectors>>,
    pub full_scan_vectors: Option<HashMap<String, FullScanVectors>>,
    pub scan_wait: Option<Duration>,
    pub signature: Option<bool>,
    pub statement: Option<String>,
    pub timeout: Option<Duration>,
    pub tx_data: Option<Vec<u8>>,
    pub tx_id: Option<String>,
    pub tx_implicit: Option<bool>,
    pub tx_stmt_num: Option<u32>,
    pub tx_timeout: Option<Duration>,
    pub use_cbo: Option<bool>,
    pub use_fts: Option<bool>,
    pub use_replica: Option<ReplicaLevel>,

    pub named_args: Option<HashMap<String, Vec<u8>>>,
    pub raw: Option<HashMap<String, Vec<u8>>>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
}

impl Serialize for QueryOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use helpers::durations;
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;

        macro_rules! serialize_if_not_none {
            ($field:expr, $name:expr) => {
                if !$field.is_none() {
                    map.serialize_entry($name, &$field)?;
                }
            };
        }

        macro_rules! serialize_duration_if_not_none {
            ($field:expr, $name:expr) => {
                if !$field.is_none() {
                    map.serialize_entry(
                        $name,
                        &durations::duration_to_golang_string(&$field.unwrap()),
                    )?;
                }
            };
        }

        serialize_if_not_none!(self.args, "args");
        serialize_if_not_none!(self.atr_collection, "atr_collection");
        serialize_if_not_none!(self.auto_execute, "auto_execute");
        serialize_if_not_none!(self.client_context_id, "client_context_id");
        serialize_if_not_none!(self.compression, "compression");
        serialize_if_not_none!(self.controls, "controls");
        serialize_if_not_none!(self.creds, "creds");
        serialize_if_not_none!(self.durability_level, "durability_level");
        serialize_if_not_none!(self.encoded_plan, "encoded_plan");
        serialize_if_not_none!(self.encoding, "encoding");
        serialize_if_not_none!(self.format, "format");
        serialize_duration_if_not_none!(self.kv_timeout, "kvtimeout");
        serialize_if_not_none!(self.max_parallelism, "max_parallelism");
        serialize_if_not_none!(self.memory_quota, "memory_quota");
        serialize_if_not_none!(self.metrics, "metrics");
        serialize_if_not_none!(self.namespace, "namespace");
        serialize_if_not_none!(self.num_atrs, "num_atrs");
        serialize_if_not_none!(self.pipeline_batch, "pipeline_batch");
        serialize_if_not_none!(self.pipeline_cap, "pipeline_cap");
        serialize_if_not_none!(self.prepared, "prepared");
        serialize_if_not_none!(self.preserve_expiry, "preserve_expiry");
        serialize_if_not_none!(self.pretty, "pretty");
        serialize_if_not_none!(self.profile, "profile");
        serialize_if_not_none!(self.query_context, "query_context");
        serialize_if_not_none!(self.read_only, "read_only");
        serialize_if_not_none!(self.scan_cap, "scan_cap");
        serialize_if_not_none!(self.scan_consistency, "scan_consistency");
        serialize_if_not_none!(self.sparse_scan_vector, "scan_vector");
        serialize_if_not_none!(self.full_scan_vector, "scan_vector");
        serialize_if_not_none!(self.sparse_scan_vectors, "scan_vectors");
        serialize_if_not_none!(self.full_scan_vectors, "scan_vectors");
        serialize_duration_if_not_none!(self.scan_wait, "scan_wait");
        serialize_if_not_none!(self.signature, "signature");
        serialize_if_not_none!(self.statement, "statement");
        serialize_duration_if_not_none!(self.timeout, "timeout");
        serialize_if_not_none!(self.tx_data, "txdata");
        serialize_if_not_none!(self.tx_id, "txid");
        serialize_if_not_none!(self.tx_implicit, "tximplicit");
        serialize_if_not_none!(self.tx_stmt_num, "txstmtnum");
        serialize_duration_if_not_none!(self.tx_timeout, "txtimeout");
        serialize_if_not_none!(self.use_cbo, "use_cbo");
        serialize_if_not_none!(self.use_fts, "use_fts");
        serialize_if_not_none!(self.use_replica, "use_replica");

        if let Some(args) = &self.named_args {
            // Prefix each named_arg with "$" if not already prefixed.
            for (key, value) in args {
                let key = if key.starts_with('$') {
                    key
                } else {
                    &format!("${}", key)
                };
                map.serialize_entry(key, value)?;
            }
        }

        if let Some(raw) = &self.raw {
            // Move raw fields to the top level.
            for (key, value) in raw {
                map.serialize_entry(key, value)?;
            }
        }

        map.end()
    }
}
