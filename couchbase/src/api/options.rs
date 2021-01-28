use crate::api::MutationState;
use serde::Serializer;
use serde_derive::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// Macro to DRY up the repetitive timeout setter.
macro_rules! timeout {
    () => {
        pub fn timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }
    };
}

macro_rules! expiry {
    () => {
        pub fn expiry(mut self, expiry: Duration) -> Self {
            self.expiry = Some(expiry);
            self
        }
    };
}

#[derive(Debug, Default, Serialize)]
pub struct QueryOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_consistency: Option<QueryScanConsistency>,
    #[serde(skip)]
    pub(crate) adhoc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(serialize_with = "default_client_context_id")]
    pub(crate) client_context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_batch: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) scan_wait: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) readonly: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<QueryProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_mutation_state")]
    pub(crate) consistent_with: Option<MutationState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "args")]
    pub(crate) positional_parameters: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "convert_named_params")]
    pub(crate) named_parameters: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The statement is not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the query engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    pub(crate) statement: Option<String>,
}

fn convert_mutation_state<S>(_x: &Option<MutationState>, _s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    todo!()
}

fn convert_duration_for_golang<S>(x: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!(
        "{}ms",
        x.expect("Expected a duration!").as_millis()
    ))
}

fn default_client_context_id<S>(x: &Option<String>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if x.is_some() {
        s.serialize_str(x.as_ref().unwrap())
    } else {
        s.serialize_str(&format!("{}", Uuid::new_v4()))
    }
}

fn convert_named_params<S>(
    x: &Option<serde_json::Map<String, Value>>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        Some(m) => {
            let conv: HashMap<String, &Value> =
                m.iter().map(|(k, v)| (format!("${}", k), v)).collect();
            s.serialize_some(&conv)
        }
        None => s.serialize_none(),
    }
}

impl QueryOptions {
    timeout!();

    pub fn scan_consistency(mut self, scan_consistency: QueryScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn adhoc(mut self, adhoc: bool) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub fn client_context_id(mut self, client_context_id: String) -> Self {
        self.client_context_id = Some(client_context_id);
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    pub fn pipeline_batch(mut self, pipeline_batch: u32) -> Self {
        self.pipeline_batch = Some(pipeline_batch);
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: u32) -> Self {
        self.pipeline_cap = Some(pipeline_cap);
        self
    }

    pub fn scan_cap(mut self, scan_cap: u32) -> Self {
        self.scan_cap = Some(scan_cap);
        self
    }

    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = Some(readonly);
        self
    }

    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn profile(mut self, profile: QueryProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn consistent_with(mut self, consistent_with: MutationState) -> Self {
        self.consistent_with = Some(consistent_with);
        self
    }

    pub fn positional_parameters<T>(mut self, positional_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let positional_parameters = match serde_json::to_value(positional_parameters) {
            Ok(Value::Array(a)) => a,
            Ok(_) => panic!("Only arrays are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.positional_parameters = Some(positional_parameters);
        self
    }

    pub fn named_parameters<T>(mut self, named_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let named_parameters = match serde_json::to_value(named_parameters) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.named_parameters = Some(named_parameters);
        self
    }

    pub fn raw<T>(mut self, raw: T) -> Self
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode raw parameters"),
        };
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Serialize)]
pub enum QueryScanConsistency {
    #[serde(rename = "not_bounded")]
    NotBounded,
    #[serde(rename = "request_plus")]
    RequestPlus,
}

#[derive(Debug, Serialize)]
pub enum QueryProfile {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "phases")]
    Phases,
    #[serde(rename = "timings")]
    Timings,
}

#[derive(Debug, Default, Serialize)]
pub struct AnalyticsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_consistency: Option<AnalyticsScanConsistency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(serialize_with = "default_client_context_id")]
    pub(crate) client_context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "args")]
    pub(crate) positional_parameters: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "convert_named_params")]
    pub(crate) named_parameters: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) readonly: Option<bool>,
    #[serde(skip)]
    pub(crate) priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The statement is not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the analytics engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    pub(crate) statement: Option<String>,
}

impl AnalyticsOptions {
    timeout!();

    pub fn scan_consistency(mut self, scan_consistency: AnalyticsScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn client_context_id(mut self, client_context_id: String) -> Self {
        self.client_context_id = Some(client_context_id);
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = Some(readonly);
        self
    }

    pub fn positional_parameters<T>(mut self, positional_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let positional_parameters = match serde_json::to_value(positional_parameters) {
            Ok(Value::Array(a)) => a,
            Ok(_) => panic!("Only arrays are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.positional_parameters = Some(positional_parameters);
        self
    }

    pub fn named_parameters<T>(mut self, named_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let named_parameters = match serde_json::to_value(named_parameters) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.named_parameters = Some(named_parameters);
        self
    }

    pub fn priority(mut self, priority: bool) -> Self {
        self.priority = Some(if priority { -1 } else { 0 });
        self
    }

    pub fn raw<T>(mut self, raw: T) -> Self
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode raw parameters"),
        };
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Serialize)]
pub enum AnalyticsScanConsistency {
    #[serde(rename = "not_bounded")]
    NotBounded,
    #[serde(rename = "request_plus")]
    RequestPlus,
}

#[derive(Debug, Default, Serialize)]
pub struct SearchOptions {
    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<u32>,
    #[serde(rename = "from")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) skip: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) explain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The query and index are not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the search engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    #[serde(rename = "indexName")]
    pub(crate) index: Option<String>,
    pub(crate) query: Option<serde_json::Value>,
}

impl SearchOptions {
    timeout!();

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }

    pub fn explain(mut self, explain: bool) -> Self {
        self.explain = Some(explain);
        self
    }

    pub fn raw<T>(mut self, raw: T) -> Self
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode raw parameters"),
        };
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Default)]
pub struct GetOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAndTouchOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndTouchOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAndLockOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndLockOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
}

impl UpsertOptions {
    timeout!();
    expiry!();
}

#[derive(Debug, Default)]
pub struct InsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
}

impl InsertOptions {
    timeout!();
    expiry!();
}

#[derive(Debug, Default)]
pub struct ReplaceOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
}

impl ReplaceOptions {
    timeout!();
    expiry!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct RemoveOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl RemoveOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct ExistsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl ExistsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct AppendOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl AppendOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct PrependOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
}

impl PrependOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct IncrementOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: Option<u64>,
}

impl IncrementOptions {
    timeout!();
    expiry!();

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct DecrementOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: Option<u64>,
}

impl DecrementOptions {
    timeout!();
    expiry!();

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub(crate) struct CounterOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) delta: i64,
}

#[derive(Debug, Default)]
pub struct MutateInOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) store_semantics: Option<StoreSemantics>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) access_deleted: Option<bool>,
}

impl MutateInOptions {
    timeout!();
    expiry!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn store_semantics(mut self, store_semantics: StoreSemantics) -> Self {
        self.store_semantics = Some(store_semantics);
        self
    }

    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }
}

/// Describes how the outer document store semantics on subdoc should act.
#[derive(Debug)]
pub enum StoreSemantics {
    /// Create the document, fail if it exists.
    Insert,
    /// Replace the document or create it if it does not exist.
    Upsert,
    /// Replace the document, fail if it does not exist. This is the default.
    Replace,
}

#[derive(Debug, Default)]
pub struct LookupInOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) access_deleted: Option<bool>,
}

impl LookupInOptions {
    timeout!();

    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }
}

macro_rules! domain_name {
    () => {
        pub fn domain_name(mut self, domain_name: String) -> Self {
            self.domain_name = Some(domain_name);
            self
        }
    };
}

#[derive(Debug, Default)]
pub struct GetUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl GetUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct GetAllUsersOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl GetAllUsersOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct UpsertUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl UpsertUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct DropUserOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) domain_name: Option<String>,
}

impl DropUserOptions {
    timeout!();
    domain_name!();
}

#[derive(Debug, Default)]
pub struct GetRolesOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetRolesOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetGroupOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAllGroupsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllGroupsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl UpsertGroupOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropGroupOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropGroupOptions {
    timeout!();
}

#[derive(Debug, Default)]
#[cfg(feature = "volatile")]
pub struct KvStatsOptions {
    pub(crate) timeout: Option<Duration>,
}

#[cfg(feature = "volatile")]
impl KvStatsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct PingOptions {
    pub(crate) report_id: Option<String>,
}

impl PingOptions {
    pub fn report_id(mut self, report_id: String) -> Self {
        self.report_id = Some(report_id);
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAllScopesOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllScopesOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateScopeOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateScopeOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateCollectionOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateCollectionOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropScopeOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropScopeOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropCollectionOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropCollectionOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct CreateBucketOptions {
    pub(crate) timeout: Option<Duration>,
}

impl CreateBucketOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct DropBucketOptions {
    pub(crate) timeout: Option<Duration>,
}

impl DropBucketOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpdateBucketOptions {
    pub(crate) timeout: Option<Duration>,
}

impl UpdateBucketOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetBucketOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetBucketOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAllBucketsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllBucketsOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct FlushBucketOptions {
    pub(crate) timeout: Option<Duration>,
}

impl FlushBucketOptions {
    timeout!();
}
