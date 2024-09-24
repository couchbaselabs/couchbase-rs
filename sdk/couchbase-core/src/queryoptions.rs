use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use typed_builder::TypedBuilder;

use crate::httpx::request::OnBehalfOfInfo;
use crate::queryx;
use crate::retry::RetryStrategy;
use crate::retryfailfast::FailFastRetryStrategy;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct QueryOptions {
    pub args: Vec<Vec<u8>>,
    pub atr_collection: Option<String>,
    pub auto_execute: Option<bool>,
    pub client_context_id: Option<String>,
    pub compression: Option<crate::queryx::query_options::Compression>,
    pub controls: Option<bool>,
    pub creds: Option<Vec<crate::queryx::query_options::CredsJson>>,
    pub durability_level: Option<crate::queryx::query_options::DurabilityLevel>,
    pub encoded_plan: Option<String>,
    pub encoding: Option<crate::queryx::query_options::Encoding>,
    pub format: Option<crate::queryx::query_options::Format>,
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
    pub profile: Option<crate::queryx::query_options::ProfileMode>,
    pub query_context: Option<String>,
    pub read_only: Option<bool>,
    pub scan_cap: Option<u32>,
    pub scan_consistency: Option<crate::queryx::query_options::ScanConsistency>,
    pub scan_vector: Vec<u8>,
    pub scan_vectors: HashMap<String, Vec<u8>>,
    pub scan_wait: Option<Duration>,
    pub signature: Option<bool>,
    pub statement: Option<String>,
    pub timeout: Option<Duration>,
    pub tx_data: Vec<u8>,
    pub tx_id: Option<String>,
    pub tx_implicit: Option<bool>,
    pub tx_stmt_num: Option<u32>,
    pub tx_timeout: Option<Duration>,
    pub use_cbo: Option<bool>,
    pub use_fts: Option<bool>,
    pub use_replica: Option<crate::queryx::query_options::ReplicaLevel>,

    pub named_args: HashMap<String, Vec<u8>>,
    pub raw: HashMap<String, Vec<u8>>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    #[builder(default=Arc::new(FailFastRetryStrategy::default()))]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl From<QueryOptions> for queryx::query_options::QueryOptions {
    fn from(opts: QueryOptions) -> Self {
        queryx::query_options::QueryOptions {
            args: opts.args,
            atr_collection: opts.atr_collection,
            auto_execute: opts.auto_execute,
            client_context_id: opts.client_context_id,
            compression: opts.compression,
            controls: opts.controls,
            creds: opts.creds,
            durability_level: opts.durability_level,
            encoded_plan: opts.encoded_plan,
            encoding: opts.encoding,
            format: opts.format,
            kv_timeout: opts.kv_timeout,
            max_parallelism: opts.max_parallelism,
            memory_quota: opts.memory_quota,
            metrics: opts.metrics,
            namespace: opts.namespace,
            num_atrs: opts.num_atrs,
            pipeline_batch: opts.pipeline_batch,
            pipeline_cap: opts.pipeline_cap,
            prepared: opts.prepared,
            preserve_expiry: opts.preserve_expiry,
            pretty: opts.pretty,
            profile: opts.profile,
            query_context: opts.query_context,
            read_only: opts.read_only,
            scan_cap: opts.scan_cap,
            scan_consistency: opts.scan_consistency,
            scan_vector: opts.scan_vector,
            scan_vectors: opts.scan_vectors,
            scan_wait: opts.scan_wait,
            signature: opts.signature,
            statement: opts.statement,
            timeout: opts.timeout,
            tx_data: opts.tx_data,
            tx_id: opts.tx_id,
            tx_implicit: opts.tx_implicit,
            tx_stmt_num: opts.tx_stmt_num,
            tx_timeout: opts.tx_timeout,
            use_cbo: opts.use_cbo,
            use_fts: opts.use_fts,
            use_replica: opts.use_replica,
            named_args: opts.named_args,
            raw: opts.raw,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
