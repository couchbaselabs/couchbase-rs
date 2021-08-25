use crate::api::MutationState;
use crate::{CouchbaseResult, SearchSort, CouchbaseError, ErrorContext, SearchFacet};
use serde::{Serializer};
use serde_derive::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;
use std::fmt::{Display, Formatter};


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

impl From<&GetAllQueryIndexOptions> for QueryOptions {
    fn from(opts: &GetAllQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&CreateQueryIndexOptions> for QueryOptions {
    fn from(opts: &CreateQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&CreatePrimaryQueryIndexOptions> for QueryOptions {
    fn from(opts: &CreatePrimaryQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&DropQueryIndexOptions> for QueryOptions {
    fn from(opts: &DropQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&DropPrimaryQueryIndexOptions> for QueryOptions {
    fn from(opts: &DropPrimaryQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

impl From<&BuildDeferredQueryIndexOptions> for QueryOptions {
    fn from(opts: &BuildDeferredQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
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

#[derive(Debug, Clone, Serialize)]
pub enum SearchHighlightStyle {
    #[serde(rename = "html")]
    HTML,
    #[serde(rename = "ansi")]
    ANSI
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchHighLight {
    #[serde(rename = "style")]
    #[serde(skip_serializing_if = "Option::is_none")]
    style: Option<SearchHighlightStyle>,
    #[serde(rename = "fields")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SearchScanConsistency {
    NotBounded
}

// No idea why it won't let me do this as derive
impl Display for SearchScanConsistency {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            SearchScanConsistency::NotBounded => write!(f, "not_bounded"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchCtlConsistency {
    level: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    vectors: HashMap<String, HashMap<String, u64>>
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchCtl {
    ctl: SearchCtlConsistency
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
    pub(crate) highlight: Option<SearchHighLight>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consistency: Option<SearchCtl>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facets: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The query and index are not part of the public API, but added here
    // as a convenience so we can convert the whole block into the
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

    pub fn highlight(mut self, style: Option<SearchHighlightStyle>, fields: Vec<String>) -> Self {
        self.highlight = Some(SearchHighLight{
            style,
            fields,
        });
        self
    }

    pub fn fields<I, T>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>
    {
        self.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn scan_consistency(mut self, level: SearchScanConsistency) -> Self {
        self.consistency = Some(SearchCtl{
            ctl: SearchCtlConsistency{
                level: level.to_string(),
                vectors: HashMap::new(),
            }
        });
        self
    }

    pub fn consistent_with(mut self, state: MutationState) -> Self {
        let mut vectors = HashMap::new();
        for token in state.tokens.into_iter().next() {
            let bucket  = token.bucket_name().to_string();
            if !vectors.contains_key(&bucket) {
                vectors.insert(bucket.clone(), HashMap::new());
            }

            let vector = vectors.get_mut(&bucket).unwrap();
            vector.insert(format!("{}/{}", token.partition_uuid(), token.partition_id()), token.sequence_number());
        }
        self.consistency = Some(SearchCtl{
            ctl: SearchCtlConsistency{
                level: "at_plus".into(),
                vectors,
            }
        });
        self
    }

    pub fn sort<T>(mut self, sort: Vec<T>) -> Self
    where
        T: SearchSort
    {
        let jsonified = serde_json::to_value(sort).map_err(|e| CouchbaseError::EncodingFailure {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
            ctx: ErrorContext::default(),
        }).unwrap();
        self.sort = Some(jsonified);
        self
    }

    pub fn facets<T>(mut self, facets: HashMap<String, T>) -> Self
        where
            T: SearchFacet
    {
        let jsonified = serde_json::to_value(facets).map_err(|e| CouchbaseError::EncodingFailure {
            source: std::io::Error::new(std::io::ErrorKind::Other, e),
            ctx: ErrorContext::default(),
        }).unwrap();
        self.facets = Some(jsonified);
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

#[derive(Debug, Clone, Copy)]
pub enum ViewScanConsistency {
    NotBounded,
    RequestPlus,
    UpdateAfter,
}

#[derive(Debug, Clone, Copy)]
pub enum ViewOrdering {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy)]
pub enum ViewErrorMode {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy)]
pub enum DesignDocumentNamespace {
    Production,
    Development,
}

#[derive(Debug, Default)]
pub struct ViewOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) scan_consistency: Option<ViewScanConsistency>,
    pub(crate) skip: Option<u32>,
    pub(crate) limit: Option<u32>,
    pub(crate) order: Option<ViewOrdering>,
    pub(crate) reduce: Option<bool>,
    pub(crate) group: Option<bool>,
    pub(crate) group_level: Option<u32>,
    pub(crate) key: Option<Value>,
    pub(crate) keys: Option<Value>,
    pub(crate) start_key: Option<Value>,
    pub(crate) end_key: Option<Value>,
    pub(crate) inclusive_end: Option<bool>,
    pub(crate) start_key_doc_id: Option<String>,
    pub(crate) end_key_doc_id: Option<String>,
    pub(crate) on_error: Option<ViewErrorMode>,
    pub(crate) debug: Option<bool>,
    pub(crate) namespace: Option<DesignDocumentNamespace>,
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
}

impl ViewOptions {
    timeout!();

    pub(crate) fn form_data(&self) -> CouchbaseResult<Vec<(&str, String)>> {
        let mut form = vec![];
        if let Some(s) = self.scan_consistency {
            match s {
                ViewScanConsistency::NotBounded => form.push(("stale", "ok".into())),
                ViewScanConsistency::RequestPlus => form.push(("stale", "false".into())),
                ViewScanConsistency::UpdateAfter => form.push(("stale", "update_after".into())),
            }
        }
        if let Some(s) = self.skip {
            form.push(("skip", s.to_string()));
        }
        if let Some(l) = self.limit {
            form.push(("limit", l.to_string()));
        }
        if let Some(o) = self.order {
            match o {
                ViewOrdering::Ascending => form.push(("descending", "false".into())),
                ViewOrdering::Descending => form.push(("descending", "falstruee".into())),
            }
        }
        if let Some(r) = self.reduce {
            if r {
                form.push(("reduce", "true".into()));

                if let Some(g) = self.group {
                    if g {
                        form.push(("group", "true".into()));
                    }
                }
                if let Some(g) = self.group_level {
                    form.push(("group_level", g.to_string()));
                }
            } else {
                form.push(("reduce", "false".into()));
            }
        }
        if let Some(k) = &self.key {
            form.push(("key", k.to_string()));
        }
        if let Some(ks) = &self.keys {
            form.push(("keys", ks.to_string()));
        }
        if let Some(k) = &self.start_key {
            form.push(("start_key", k.to_string()));
        }
        if let Some(k) = &self.end_key {
            form.push(("end_key", k.to_string()));
        }
        if self.start_key.is_some() || self.end_key.is_some() {
            if let Some(i) = self.inclusive_end {
                match i {
                    true => form.push(("inclusive_end", "true".into())),
                    false => form.push(("inclusive_end", "false".into())),
                }
            }
        }
        if let Some(k) = &self.start_key_doc_id {
            form.push(("startkey_docid", k.into()));
        }
        if let Some(k) = &self.end_key_doc_id {
            form.push(("endkey_docid", k.into()));
        }
        if let Some(o) = &self.on_error {
            match o {
                ViewErrorMode::Continue => form.push(("on_error", "continue".into())),
                ViewErrorMode::Stop => form.push(("on_error", "stop".into())),
            }
        }
        if let Some(d) = self.debug {
            if d {
                form.push(("debug", "true".into()));
            }
        }
        // if let Some(r) = &self.raw {
        //
        // }

        Ok(form)
    }

    pub fn scan_consistency(mut self, consistency: ViewScanConsistency) -> Self {
        self.scan_consistency = Some(consistency);
        self
    }
    pub fn skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn order(mut self, ordering: ViewOrdering) -> Self {
        self.order = Some(ordering);
        self
    }
    pub fn reduce(mut self, enabled: bool) -> Self {
        self.reduce = Some(enabled);
        self
    }
    pub fn group(mut self, enabled: bool) -> Self {
        self.group = Some(enabled);
        self
    }
    pub fn group_level(mut self, level: u32) -> Self {
        self.group_level = Some(level);
        self
    }
    pub fn key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode key"),
        };
        self.key = Some(k);
        self
    }
    pub fn keys<T>(mut self, keys: Vec<T>) -> Self
    where
        T: serde::Serialize,
    {
        let ks = match serde_json::to_value(keys) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode keys"),
        };
        self.keys = Some(ks);
        self
    }
    pub fn start_key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode start_key"),
        };
        self.start_key = Some(k);
        self
    }
    pub fn end_key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode end_key"),
        };
        self.end_key = Some(k);
        self
    }
    pub fn inclusive_end(mut self, inclusive_end: bool) -> Self {
        self.inclusive_end = Some(inclusive_end);
        self
    }
    pub fn start_key_doc_id(mut self, doc_id: impl Into<String>) -> Self {
        self.start_key_doc_id = Some(doc_id.into());
        self
    }
    pub fn end_key_doc_id(mut self, doc_id: impl Into<String>) -> Self {
        self.end_key_doc_id = Some(doc_id.into());
        self
    }
    pub fn on_error(mut self, error_mode: ViewErrorMode) -> Self {
        self.on_error = Some(error_mode);
        self
    }
    pub fn debug(mut self, enabled: bool) -> Self {
        self.debug = Some(enabled);
        self
    }
    pub fn namespace(mut self, namespace: DesignDocumentNamespace) -> Self {
        self.namespace = Some(namespace);
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
pub struct CreateQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_exists: Option<bool>,
    pub(crate) with: Value,
}

impl CreateQueryIndexOptions {
    timeout!();
    pub fn ignore_if_exists(mut self, ignore_exists: bool) -> Self {
        self.ignore_exists = Some(ignore_exists);
        self
    }
    pub fn num_replicas(mut self, num_replicas: i32) -> Self {
        self.with["num_replica"] = Value::from(num_replicas);
        self
    }
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.with["defer_build"] = Value::from(deferred);
        self
    }
}

#[derive(Debug, Default)]
pub struct GetAllQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAllQueryIndexOptions {
    timeout!();
}

impl From<&BuildDeferredQueryIndexOptions> for GetAllQueryIndexOptions {
    fn from(opts: &BuildDeferredQueryIndexOptions) -> Self {
        let mut us = Self::default();
        if let Some(t) = opts.timeout {
            us = us.timeout(t);
        }

        us
    }
}

#[derive(Debug, Default)]
pub struct CreatePrimaryQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_exists: Option<bool>,
    pub(crate) name: Option<String>,
    pub(crate) with: Value,
}

impl CreatePrimaryQueryIndexOptions {
    timeout!();
    pub fn ignore_if_exists(mut self, ignore_exists: bool) -> Self {
        self.ignore_exists = Some(ignore_exists);
        self
    }
    pub fn num_replicas(mut self, num_replicas: i32) -> Self {
        self.with["num_replica"] = Value::from(num_replicas);
        self
    }
    pub fn index_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.with["defer_build"] = Value::from(deferred);
        self
    }
}

#[derive(Debug, Default)]
pub struct DropQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_not_exists: Option<bool>,
}

impl DropQueryIndexOptions {
    timeout!();
    pub fn ignore_if_not_exists(mut self, ignore_not_exists: bool) -> Self {
        self.ignore_not_exists = Some(ignore_not_exists);
        self
    }
}

#[derive(Debug, Default)]
pub struct DropPrimaryQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) ignore_not_exists: Option<bool>,
    pub(crate) name: Option<String>,
}

impl DropPrimaryQueryIndexOptions {
    timeout!();
    pub fn ignore_if_not_exists(mut self, ignore_not_exists: bool) -> Self {
        self.ignore_not_exists = Some(ignore_not_exists);
        self
    }
    pub fn index_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct BuildDeferredQueryIndexOptions {
    pub(crate) timeout: Option<Duration>,
}

impl BuildDeferredQueryIndexOptions {
    timeout!();
}
