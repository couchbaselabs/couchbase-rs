use crate::mgmtx::bucket_settings_json::BucketSettingsJson;
use std::fmt::Display;
use std::string::ToString;
use std::time::Duration;
use url::form_urlencoded::Serializer;

#[derive(Default, Debug, Clone, PartialOrd, PartialEq, Eq)]
pub struct BucketSettings {
    pub flush_enabled: Option<bool>,
    pub ram_quota_mb: Option<u64>,
    pub replica_number: Option<u32>,
    pub eviction_policy: Option<EvictionPolicyType>,
    pub max_ttl: Option<Duration>,
    pub compression_mode: Option<CompressionMode>,
    pub durability_min_level: Option<DurabilityLevel>,
    pub history_retention_collection_default: Option<bool>,
    pub history_retention_bytes: Option<u64>,
    pub history_retention_seconds: Option<u32>,
    pub conflict_resolution_type: Option<ConflictResolutionType>,
    pub replica_index: Option<bool>,
    pub bucket_type: Option<BucketType>,
    pub storage_backend: Option<StorageBackend>,
    pub num_vbuckets: Option<u16>,
}

impl BucketSettings {
    pub fn flush_enabled(mut self, flush_enabled: bool) -> Self {
        self.flush_enabled = Some(flush_enabled);
        self
    }

    pub fn ram_quota_mb(mut self, ram_quota_mb: u64) -> Self {
        self.ram_quota_mb = Some(ram_quota_mb);
        self
    }

    pub fn replica_number(mut self, replica_number: u32) -> Self {
        self.replica_number = Some(replica_number);
        self
    }

    pub fn eviction_policy(mut self, eviction_policy: impl Into<EvictionPolicyType>) -> Self {
        self.eviction_policy = Some(eviction_policy.into());
        self
    }

    pub fn max_ttl(mut self, max_ttl: Duration) -> Self {
        self.max_ttl = Some(max_ttl);
        self
    }

    pub fn compression_mode(mut self, compression_mode: impl Into<CompressionMode>) -> Self {
        self.compression_mode = Some(compression_mode.into());
        self
    }

    pub fn durability_min_level(
        mut self,
        durability_min_level: impl Into<DurabilityLevel>,
    ) -> Self {
        self.durability_min_level = Some(durability_min_level.into());
        self
    }

    pub fn history_retention_collection_default(
        mut self,
        history_retention_collection_default: bool,
    ) -> Self {
        self.history_retention_collection_default = Some(history_retention_collection_default);
        self
    }

    pub fn history_retention_bytes(mut self, history_retention_bytes: u64) -> Self {
        self.history_retention_bytes = Some(history_retention_bytes);
        self
    }

    pub fn history_retention_seconds(mut self, history_retention_seconds: u32) -> Self {
        self.history_retention_seconds = Some(history_retention_seconds);
        self
    }

    pub fn conflict_resolution_type(
        mut self,
        conflict_resolution_type: impl Into<ConflictResolutionType>,
    ) -> Self {
        self.conflict_resolution_type = Some(conflict_resolution_type.into());
        self
    }

    pub fn replica_index(mut self, replica_index: bool) -> Self {
        self.replica_index = Some(replica_index);
        self
    }

    pub fn bucket_type(mut self, bucket_type: impl Into<BucketType>) -> Self {
        self.bucket_type = Some(bucket_type.into());
        self
    }

    pub fn storage_backend(mut self, storage_backend: impl Into<StorageBackend>) -> Self {
        self.storage_backend = Some(storage_backend.into());
        self
    }

    pub fn num_vbuckets(mut self, num_vbuckets: u16) -> Self {
        self.num_vbuckets = Some(num_vbuckets);
        self
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct BucketDef {
    pub name: String,
    pub bucket_settings: BucketSettings,
}

impl BucketDef {
    pub fn new(name: String, bucket_settings: BucketSettings) -> Self {
        Self {
            name,
            bucket_settings,
        }
    }
}

impl From<BucketSettingsJson> for BucketDef {
    fn from(settings: BucketSettingsJson) -> Self {
        Self {
            name: settings.name,
            bucket_settings: BucketSettings {
                flush_enabled: settings.controllers.as_ref().map(|c| {
                    if let Some(f) = &c.flush {
                        !f.is_empty()
                    } else {
                        false
                    }
                }),
                ram_quota_mb: Some(settings.quota.raw_ram / 1024 / 1024),
                replica_number: settings.replica_number,
                eviction_policy: settings.eviction_policy,
                max_ttl: settings.max_ttl.map(|d| Duration::from_secs(d as u64)),
                compression_mode: settings.compression_mode,
                durability_min_level: settings.durability_min_level,
                history_retention_collection_default: settings.history_retention_collection_default,
                history_retention_bytes: settings.history_retention_bytes,
                history_retention_seconds: settings.history_retention_seconds,
                conflict_resolution_type: settings.conflict_resolution_type,
                replica_index: settings.replica_index,
                bucket_type: settings.bucket_type,
                storage_backend: settings.storage_backend,
                num_vbuckets: settings.num_vbuckets,
            },
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BucketType(InnerBucketType);

impl BucketType {
    pub const COUCHBASE: BucketType = BucketType(InnerBucketType::Couchbase);

    pub const EPHEMERAL: BucketType = BucketType(InnerBucketType::Ephemeral);

    pub(crate) fn other(val: String) -> BucketType {
        BucketType(InnerBucketType::Other(val))
    }
}

impl Display for BucketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerBucketType::Couchbase => write!(f, "membase"),
            InnerBucketType::Ephemeral => write!(f, "ephemeral"),
            InnerBucketType::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerBucketType {
    Couchbase,
    Ephemeral,
    Other(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EvictionPolicyType(InnerEvictionPolicyType);

impl EvictionPolicyType {
    pub const VALUE_ONLY: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::ValueOnly);

    pub const FULL: EvictionPolicyType = EvictionPolicyType(InnerEvictionPolicyType::Full);

    pub const NOT_RECENTLY_USED: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::NotRecentlyUsed);

    pub const NO_EVICTION: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::NoEviction);

    pub(crate) fn other(val: String) -> EvictionPolicyType {
        EvictionPolicyType(InnerEvictionPolicyType::Other(val))
    }
}

impl Display for EvictionPolicyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerEvictionPolicyType::ValueOnly => write!(f, "valueOnly"),
            InnerEvictionPolicyType::Full => write!(f, "fullEviction"),
            InnerEvictionPolicyType::NotRecentlyUsed => write!(f, "nruEviction"),
            InnerEvictionPolicyType::NoEviction => write!(f, "noEviction"),
            InnerEvictionPolicyType::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerEvictionPolicyType {
    ValueOnly,
    Full,
    NotRecentlyUsed,
    NoEviction,
    Other(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CompressionMode(InnerCompressionMode);

impl CompressionMode {
    pub const OFF: CompressionMode = CompressionMode(InnerCompressionMode::Off);

    pub const PASSIVE: CompressionMode = CompressionMode(InnerCompressionMode::Passive);

    pub const ACTIVE: CompressionMode = CompressionMode(InnerCompressionMode::Active);

    pub(crate) fn other(val: String) -> CompressionMode {
        CompressionMode(InnerCompressionMode::Other(val))
    }
}

impl Display for CompressionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerCompressionMode::Off => write!(f, "off"),
            InnerCompressionMode::Passive => write!(f, "passive"),
            InnerCompressionMode::Active => write!(f, "active"),
            InnerCompressionMode::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerCompressionMode {
    Off,
    Passive,
    Active,
    Other(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DurabilityLevel(InnerDurabilityLevel);

impl DurabilityLevel {
    pub const NONE: DurabilityLevel = DurabilityLevel(InnerDurabilityLevel::None);

    pub const MAJORITY: DurabilityLevel = DurabilityLevel(InnerDurabilityLevel::Majority);

    pub const MAJORITY_AND_PERSIST_ACTIVE: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::MajorityAndPersistActive);

    pub const PERSIST_TO_MAJORITY: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::PersistToMajority);

    pub(crate) fn other(val: String) -> DurabilityLevel {
        DurabilityLevel(InnerDurabilityLevel::Other(val))
    }
}

impl Display for DurabilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerDurabilityLevel::None => write!(f, "none"),
            InnerDurabilityLevel::Majority => write!(f, "majority"),
            InnerDurabilityLevel::MajorityAndPersistActive => write!(f, "majorityAndPersistActive"),
            InnerDurabilityLevel::PersistToMajority => write!(f, "persistToMajority"),
            InnerDurabilityLevel::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerDurabilityLevel {
    None,
    Majority,
    MajorityAndPersistActive,
    PersistToMajority,
    Other(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ConflictResolutionType(InnerConflictResolutionType);

impl ConflictResolutionType {
    pub const SEQUENCE_NUMBER: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::SequenceNumber);

    pub const TIMESTAMP: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::Timestamp);

    pub const CUSTOM: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::Custom);

    pub(crate) fn other(val: String) -> ConflictResolutionType {
        ConflictResolutionType(InnerConflictResolutionType::Other(val))
    }
}

impl Display for ConflictResolutionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerConflictResolutionType::SequenceNumber => write!(f, "seqno"),
            InnerConflictResolutionType::Timestamp => write!(f, "lww"),
            InnerConflictResolutionType::Custom => write!(f, "custom"),
            InnerConflictResolutionType::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerConflictResolutionType {
    SequenceNumber,
    Timestamp,
    Custom,
    Other(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct StorageBackend(InnerStorageBackend);

impl StorageBackend {
    pub const COUCHSTORE: StorageBackend = StorageBackend(InnerStorageBackend::Couchstore);

    pub const MAGMA: StorageBackend = StorageBackend(InnerStorageBackend::Magma);

    pub(crate) fn other(val: String) -> StorageBackend {
        StorageBackend(InnerStorageBackend::Other(val))
    }
}

impl Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerStorageBackend::Couchstore => write!(f, "couchstore"),
            InnerStorageBackend::Magma => write!(f, "magma"),
            InnerStorageBackend::Other(val) => write!(f, "unknown({})", val),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerStorageBackend {
    Couchstore,
    Magma,
    Other(String),
}

pub(crate) fn encode_bucket_settings(serializer: &mut Serializer<String>, opts: &BucketSettings) {
    if let Some(flush) = opts.flush_enabled {
        serializer.append_pair("flushEnabled", if flush { "1" } else { "0" });
    }
    if let Some(quota) = opts.ram_quota_mb {
        serializer.append_pair("ramQuotaMB", quota.to_string().as_str());
    }
    if let Some(num) = opts.replica_number {
        serializer.append_pair("replicaNumber", num.to_string().as_str());
    }
    if let Some(eviction_policy) = &opts.eviction_policy {
        serializer.append_pair("evictionPolicy", eviction_policy.to_string().as_str());
    }
    if let Some(max_ttl) = &opts.max_ttl {
        serializer.append_pair("maxTTL", max_ttl.as_secs().to_string().as_str());
    }
    if let Some(compression_mode) = &opts.compression_mode {
        serializer.append_pair("compressionMode", compression_mode.to_string().as_str());
    }
    if let Some(durability_min_level) = &opts.durability_min_level {
        serializer.append_pair(
            "durabilityMinLevel",
            durability_min_level.to_string().as_str(),
        );
    }
    if let Some(retention) = opts.history_retention_bytes {
        serializer.append_pair("historyRetentionBytes", retention.to_string().as_str());
    }
    if let Some(retention) = opts.history_retention_seconds {
        serializer.append_pair("historyRetentionSeconds", retention.to_string().as_str());
    }
    if let Some(history_retention_collection_default) = &opts.history_retention_collection_default {
        serializer.append_pair(
            "historyRetentionCollectionDefault",
            history_retention_collection_default.to_string().as_str(),
        );
    }
    if let Some(conflict_resolution_type) = &opts.conflict_resolution_type {
        serializer.append_pair(
            "conflictResolutionType",
            conflict_resolution_type.to_string().as_str(),
        );
    }
    if let Some(index) = opts.replica_index {
        serializer.append_pair("replicaIndex", if index { "1" } else { "0" });
    }
    if let Some(bucket_type) = &opts.bucket_type {
        serializer.append_pair("bucketType", bucket_type.to_string().as_str());
    }
    if let Some(storage_backend) = &opts.storage_backend {
        serializer.append_pair("storageBackend", storage_backend.to_string().as_str());
    }
    if let Some(num_vbuckets) = opts.num_vbuckets {
        serializer.append_pair("numVBuckets", num_vbuckets.to_string().as_str());
    }
}
