use super::*;
use crate::api::collection::DurabilityLevel;
use crate::io::request::*;
use crate::CouchbaseError::{
    BucketExists, BucketNotFlushable, BucketNotFound, Generic, InvalidArgument,
};
use crate::{CouchbaseError, CouchbaseResult, ErrorContext, GenericManagementResult, ServiceType};
use futures::channel::oneshot;
use serde_derive::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BucketType {
    Couchbase,
    Memcached,
    Ephemeral,
}

impl TryFrom<&str> for BucketType {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "membase" => Ok(BucketType::Couchbase),
            "memcached" => Ok(BucketType::Memcached),
            "ephemeral" => Ok(BucketType::Ephemeral),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid bucket type".into());
                Err(Generic { ctx })
            }
        }
    }
}

impl Display for BucketType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            BucketType::Couchbase => "membase",
            BucketType::Memcached => "memcached",
            BucketType::Ephemeral => "ephemeral",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConflictResolutionType {
    Timestamp,
    SequenceNumber,
}

impl TryFrom<&str> for ConflictResolutionType {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "lww" => Ok(ConflictResolutionType::Timestamp),
            "seqno" => Ok(ConflictResolutionType::SequenceNumber),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid conflict resolution policy".into());
                Err(Generic { ctx })
            }
        }
    }
}

impl Display for ConflictResolutionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            ConflictResolutionType::Timestamp => "lww",
            ConflictResolutionType::SequenceNumber => "seqno",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EvictionPolicy {
    Full,
    ValueOnly,
    NotRecentlyUsed,
    NoEviction,
}
impl TryFrom<&str> for EvictionPolicy {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "fullEviction" => Ok(EvictionPolicy::Full),
            "valueOnly" => Ok(EvictionPolicy::ValueOnly),
            "nruEviction" => Ok(EvictionPolicy::NotRecentlyUsed),
            "noEviction" => Ok(EvictionPolicy::NoEviction),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid eviction policy".into());
                Err(Generic { ctx })
            }
        }
    }
}

impl Display for EvictionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            EvictionPolicy::Full => "fullEviction",
            EvictionPolicy::ValueOnly => "valueOnly",
            EvictionPolicy::NotRecentlyUsed => "nruEviction",
            EvictionPolicy::NoEviction => "noEviction",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompressionMode {
    Off,
    Passive,
    Active,
}

impl TryFrom<&str> for CompressionMode {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "off" => Ok(CompressionMode::Off),
            "passive" => Ok(CompressionMode::Passive),
            "active" => Ok(CompressionMode::Active),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid compression mode".into());
                Err(Generic { ctx })
            }
        }
    }
}

impl Display for CompressionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            CompressionMode::Off => "off",
            CompressionMode::Passive => "passive",
            CompressionMode::Active => "active",
        };

        write!(f, "{}", alias)
    }
}

pub struct BucketSettingsBuilder {
    name: String,
    ram_quota_mb: u64,
    flush_enabled: bool,
    num_replicas: u32,
    replica_indexes: bool,
    bucket_type: BucketType,
    eviction_policy: Option<EvictionPolicy>,
    max_expiry: Duration,
    compression_mode: CompressionMode,
    durability_level: DurabilityLevel,
    conflict_resolution_type: Option<ConflictResolutionType>,
}

impl BucketSettingsBuilder {
    pub fn new<S: Into<String>>(name: S) -> BucketSettingsBuilder {
        Self {
            name: name.into(),
            ram_quota_mb: 100,
            flush_enabled: false,
            num_replicas: 1,
            replica_indexes: false,
            bucket_type: BucketType::Couchbase,
            eviction_policy: None,
            max_expiry: Duration::from_secs(0),
            compression_mode: CompressionMode::Passive,
            durability_level: DurabilityLevel::None,
            conflict_resolution_type: None,
        }
    }

    pub fn ram_quota_mb(mut self, ram_quota_mb: u64) -> BucketSettingsBuilder {
        self.ram_quota_mb = ram_quota_mb;
        self
    }

    pub fn flush_enabled(mut self, enabled: bool) -> BucketSettingsBuilder {
        self.flush_enabled = enabled;
        self
    }

    pub fn num_replicas(mut self, num_replicas: u32) -> BucketSettingsBuilder {
        self.num_replicas = num_replicas;
        self
    }

    pub fn replica_indexes(mut self, enabled: bool) -> BucketSettingsBuilder {
        self.replica_indexes = enabled;
        self
    }

    pub fn bucket_type(mut self, bucket_type: BucketType) -> BucketSettingsBuilder {
        self.bucket_type = bucket_type;
        self
    }

    pub fn eviction_policy(mut self, eviction_policy: EvictionPolicy) -> BucketSettingsBuilder {
        self.eviction_policy = Some(eviction_policy);
        self
    }

    pub fn max_expiry(mut self, max_expiry: Duration) -> BucketSettingsBuilder {
        self.max_expiry = max_expiry;
        self
    }

    pub fn compression_mode(mut self, mode: CompressionMode) -> BucketSettingsBuilder {
        self.compression_mode = mode;
        self
    }

    pub fn minimum_durability_level(
        mut self,
        durability_level: DurabilityLevel,
    ) -> BucketSettingsBuilder {
        self.durability_level = durability_level;
        self
    }

    pub fn conflict_resolution_type(&mut self, conflict_resolution_type: ConflictResolutionType) {
        self.conflict_resolution_type = Some(conflict_resolution_type);
    }

    pub fn build(self) -> BucketSettings {
        BucketSettings {
            name: self.name,
            ram_quota_mb: self.ram_quota_mb,
            flush_enabled: self.flush_enabled,
            num_replicas: self.num_replicas,
            replica_indexes: self.replica_indexes,
            bucket_type: self.bucket_type,
            eviction_policy: self.eviction_policy,
            max_expiry: self.max_expiry,
            compression_mode: self.compression_mode,
            durability_level: self.durability_level,
            conflict_resolution_type: self.conflict_resolution_type,
        }
    }
}

#[derive(Debug)]
pub struct BucketSettings {
    name: String,
    ram_quota_mb: u64,
    flush_enabled: bool,
    num_replicas: u32,
    replica_indexes: bool,
    bucket_type: BucketType,
    eviction_policy: Option<EvictionPolicy>,
    max_expiry: Duration,
    compression_mode: CompressionMode,
    durability_level: DurabilityLevel,
    conflict_resolution_type: Option<ConflictResolutionType>,
}

#[derive(Debug, Deserialize)]
struct JSONControllers {
    #[serde(default)]
    flush: String,
}

#[derive(Debug, Deserialize)]
struct JSONQuota {
    ram: u64,
    #[serde(rename = "rawRAM")]
    raw_ram: u64,
}

#[derive(Debug, Deserialize)]
struct JSONBucketSettings {
    name: String,
    controllers: JSONControllers,
    quota: JSONQuota,
    #[serde(rename = "replicaNumber")]
    num_replicas: u32,
    #[serde(default)]
    #[serde(rename = "replicaIndex")]
    replica_indexes: bool,
    #[serde(rename = "bucketType")]
    bucket_type: String,
    #[serde(rename = "evictionPolicy")]
    eviction_policy: String,
    #[serde(rename = "maxTTL")]
    max_expiry: u32,
    #[serde(rename = "compressionMode")]
    compression_mode: String,
    #[serde(rename = "durabilityMinLevel", default)]
    durability_level: String,
    #[serde(rename = "conflictResolutionType")]
    conflict_resolution_type: String,
}

impl BucketSettings {
    fn from(settings: JSONBucketSettings) -> CouchbaseResult<BucketSettings> {
        Ok(BucketSettings {
            name: settings.name,
            ram_quota_mb: settings.quota.raw_ram / 1024 / 1024,
            flush_enabled: !settings.controllers.flush.is_empty(),
            num_replicas: settings.num_replicas,
            replica_indexes: settings.replica_indexes,
            bucket_type: BucketType::try_from(settings.bucket_type.as_str())?,
            eviction_policy: Some(EvictionPolicy::try_from(settings.eviction_policy.as_str())?),
            max_expiry: Default::default(),
            compression_mode: CompressionMode::try_from(settings.compression_mode.as_str())?,
            durability_level: DurabilityLevel::try_from(settings.durability_level.as_str())?,
            conflict_resolution_type: Some(ConflictResolutionType::try_from(
                settings.conflict_resolution_type.as_str(),
            )?),
        })
    }

    fn as_form(&self, is_update: bool) -> Result<Vec<(&str, String)>, CouchbaseError> {
        if self.ram_quota_mb < 100 {
            let mut ctx = ErrorContext::default();
            ctx.insert("ram quota must be more than 100mb", "".into());
            return Err(InvalidArgument { ctx });
        }
        let flush_enabled = match self.flush_enabled {
            true => "1",
            false => "0",
        };
        let replica_index_enabled = match self.replica_indexes {
            true => "1",
            false => "0",
        };
        let mut form = vec![
            ("name", self.name.clone()),
            ("ramQuotaMB", self.ram_quota_mb.to_string()),
            ("flushEnabled", flush_enabled.into()),
            ("bucketType", self.bucket_type.to_string()),
            ("compressionMode", self.compression_mode.to_string()),
        ];

        match self.durability_level {
            DurabilityLevel::None => {}
            DurabilityLevel::ClientVerified(_) => {
                return Err(CouchbaseError::InvalidArgument {
                    ctx: ErrorContext::from(("durability", "cannot be client verified")),
                });
            }
            _ => {
                form.push(("durabilityMinLevel", self.durability_level.to_string()));
            }
        }

        if let Some(conflict_type) = self.conflict_resolution_type {
            if !is_update {
                form.push(("conflictResolutionType", conflict_type.to_string()));
            }
        }

        match self.bucket_type {
            BucketType::Couchbase => {
                if let Some(eviction_policy) = self.eviction_policy {
                    match eviction_policy {
                        EvictionPolicy::NoEviction => {
                            let mut ctx = ErrorContext::default();
                            ctx.insert(
                                "eviction policy cannot be used with couchbase buckets",
                                "NoEviction".into(),
                            );
                            return Err(InvalidArgument { ctx });
                        }
                        EvictionPolicy::NotRecentlyUsed => {
                            let mut ctx = ErrorContext::default();
                            ctx.insert(
                                "eviction policy cannot be used with couchbase buckets",
                                "NotRecentlyUsed".into(),
                            );
                            return Err(InvalidArgument { ctx });
                        }
                        _ => {
                            form.push(("evictionPolicy", eviction_policy.to_string()));
                        }
                    }
                }
                form.push(("replicaNumber", self.num_replicas.to_string()));
                form.push(("replicaIndex", replica_index_enabled.into()));
            }
            BucketType::Ephemeral => {
                if let Some(eviction_policy) = self.eviction_policy {
                    match eviction_policy {
                        EvictionPolicy::Full => {
                            let mut ctx = ErrorContext::default();
                            ctx.insert(
                                "eviction policy cannot be used with ephemeral buckets",
                                "Full".into(),
                            );
                            return Err(InvalidArgument { ctx });
                        }
                        EvictionPolicy::ValueOnly => {
                            let mut ctx = ErrorContext::default();
                            ctx.insert(
                                "eviction policy cannot be used with ephemeral buckets",
                                "ValueOnly".into(),
                            );
                            return Err(InvalidArgument { ctx });
                        }
                        _ => {
                            form.push(("evictionPolicy", eviction_policy.to_string()));
                        }
                    }
                }
                form.push(("replicaNumber", self.num_replicas.to_string()));
            }
            BucketType::Memcached => {
                if self.num_replicas > 0 {
                    let mut ctx = ErrorContext::default();
                    ctx.insert(
                        "field cannot be used with memcached buckets",
                        "num_replicas".into(),
                    );
                    return Err(InvalidArgument { ctx });
                }
                if self.eviction_policy.is_some() {
                    let mut ctx = ErrorContext::default();
                    ctx.insert(
                        "field cannot be used with memcached buckets",
                        "eviction_policy".into(),
                    );
                    return Err(InvalidArgument { ctx });
                }
                form.push(("replicaIndex", replica_index_enabled.into()));
            }
        }

        Ok(form)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ram_quota_mb(&self) -> u64 {
        self.ram_quota_mb
    }

    pub fn flush_enabled(&self) -> bool {
        self.flush_enabled
    }

    pub fn num_replicas(&self) -> u32 {
        self.num_replicas
    }

    pub fn replica_indexes(&self) -> bool {
        self.replica_indexes
    }

    pub fn bucket_type(&self) -> BucketType {
        self.bucket_type
    }

    pub fn eviction_policy(&self) -> Option<EvictionPolicy> {
        self.eviction_policy
    }

    pub fn max_expiry(&self) -> Duration {
        self.max_expiry
    }

    pub fn compression_mode(&self) -> CompressionMode {
        self.compression_mode
    }

    pub fn minimum_durability_level(&self) -> DurabilityLevel {
        self.durability_level
    }

    pub fn set_ram_quota_mb(&mut self, ram_quota_mb: u64) {
        self.ram_quota_mb = ram_quota_mb;
    }

    pub fn set_flush_enabled(&mut self, enabled: bool) {
        self.flush_enabled = enabled;
    }

    pub fn set_num_replicas(&mut self, num_replicas: u32) {
        self.num_replicas = num_replicas;
    }

    pub fn set_replica_indexes(&mut self, enabled: bool) {
        self.replica_indexes = enabled;
    }

    pub fn set_bucket_type(&mut self, bucket_type: BucketType) {
        self.bucket_type = bucket_type;
    }

    pub fn set_eviction_policy(&mut self, eviction_policy: EvictionPolicy) {
        self.eviction_policy = Some(eviction_policy);
    }

    pub fn set_max_expiry(&mut self, max_expiry: Duration) {
        self.max_expiry = max_expiry;
    }

    pub fn set_compression_mode(&mut self, mode: CompressionMode) {
        self.compression_mode = mode;
    }

    pub fn set_minimum_durability_level(&mut self, durability_level: DurabilityLevel) {
        self.durability_level = durability_level;
    }
}

pub struct BucketManager {
    core: Arc<Core>,
}

impl BucketManager {
    pub(crate) fn new(core: Arc<Core>) -> Self {
        Self { core }
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        options: impl Into<Option<CreateBucketOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let form = settings.as_form(false)?;

        let form_encoded = serde_urlencoded::to_string(&form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: "/pools/default/buckets/".into(),
                method: String::from("post"),
                payload: Some(form_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            202 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                settings.name,
            )),
        }
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        options: impl Into<Option<UpdateBucketOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        // The server expects form data so we need to build that, serde expects each value to be an
        // Option.
        let form = settings.as_form(true)?;

        let form_encoded = serde_urlencoded::to_string(&form)?;
        let content_type = String::from("application/x-www-form-urlencoded");
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}", settings.name),
                method: String::from("post"),
                payload: Some(form_encoded),
                content_type: Some(content_type),
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                settings.name,
            )),
        }
    }

    pub async fn drop_bucket(
        &self,
        name: impl Into<String>,
        options: impl Into<Option<DropBucketOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        let bucket_name = name.into();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}", &bucket_name),
                method: String::from("delete"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                bucket_name,
            )),
        }
    }

    pub async fn get_bucket(
        &self,
        name: impl Into<String>,
        options: impl Into<Option<GetBucketOptions>>,
    ) -> CouchbaseResult<BucketSettings> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        let bucket_name = name.into();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}", &bucket_name),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        let bucket_data: JSONBucketSettings = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                bucket_name,
            )),
        }?;

        BucketSettings::from(bucket_data)
    }

    pub async fn get_all_buckets(
        &self,
        options: impl Into<Option<GetAllBucketsOptions>>,
    ) -> CouchbaseResult<HashMap<String, BucketSettings>> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: "/pools/default/buckets".into(),
                method: String::from("get"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        let bucket_data: Vec<JSONBucketSettings> = match result.http_status() {
            200 => serde_json::from_slice(result.payload_or_error()?)
                .map_err(CouchbaseError::decoding_failure_from_serde),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                "",
            )),
        }?;

        let mut settings = HashMap::new();
        for data in bucket_data {
            let name = data.name.to_owned();
            settings.insert(name, BucketSettings::from(data)?);
        }

        Ok(settings)
    }

    pub async fn flush_bucket(
        &self,
        name: impl Into<String>,
        options: impl Into<Option<FlushBucketOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();

        let bucket_name = name.into();
        self.core
            .send(Request::GenericManagement(GenericManagementRequest {
                sender,
                path: format!("/pools/default/buckets/{}/controller/doFlush", &bucket_name),
                method: String::from("post"),
                payload: None,
                content_type: None,
                timeout: options.timeout,
                service_type: Some(ServiceType::Management),
            }));

        let result: GenericManagementResult = receiver.await.unwrap()?;

        match result.http_status() {
            200 => Ok(()),
            _ => Err(self.parse_error(
                result.http_status(),
                String::from_utf8(result.payload_or_error()?.to_owned())?.to_lowercase(),
                bucket_name,
            )),
        }
    }

    fn parse_error<S: Into<String>>(
        &self,
        status: u16,
        message: String,
        bucket_name: S,
    ) -> CouchbaseError {
        if message.contains("resource not found") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(bucket_name.into()));
            return BucketNotFound { ctx };
        }
        if message.contains("bucket with given name already exists") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(bucket_name.into()));
            return BucketExists { ctx };
        }
        if message.contains("flush is disabled") {
            let mut ctx = ErrorContext::default();
            ctx.insert("name", Value::String(bucket_name.into()));
            return BucketNotFlushable { ctx };
        }

        CouchbaseError::GenericHTTP {
            ctx: Default::default(),
            status,
            message,
        }
    }
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
