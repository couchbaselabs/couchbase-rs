use crate::durability_level::DurabilityLevel;
use couchbase_core::mgmtx::bucket_settings::BucketDef;
use std::fmt::Display;
use std::time::Duration;

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BucketSettings {
    pub name: String,
    pub ram_quota_mb: u64,
    pub flush_enabled: Option<bool>,
    pub num_replicas: Option<u32>,
    pub eviction_policy: Option<EvictionPolicyType>,
    pub max_expiry: Option<Duration>,
    pub compression_mode: Option<CompressionMode>,
    pub minimum_durability_level: Option<DurabilityLevel>,
    pub history_retention_collection_default: Option<bool>,
    pub history_retention_bytes: Option<u64>,
    pub history_retention_duration: Option<Duration>,
    pub conflict_resolution_type: Option<ConflictResolutionType>,
    pub replica_indexes: Option<bool>,
    pub bucket_type: Option<BucketType>,
    pub storage_backend: Option<StorageBackend>,
    pub num_vbuckets: Option<u16>,
}

impl BucketSettings {
    pub fn new(name: impl Into<String>, ram_quota_mb: u64) -> Self {
        Self {
            name: name.into(),
            ram_quota_mb,
            flush_enabled: None,
            num_replicas: None,
            eviction_policy: None,
            max_expiry: None,
            compression_mode: None,
            minimum_durability_level: None,
            history_retention_collection_default: None,
            history_retention_bytes: None,
            history_retention_duration: None,
            conflict_resolution_type: None,
            replica_indexes: None,
            bucket_type: None,
            storage_backend: None,
            num_vbuckets: None,
        }
    }

    pub fn flush_enabled(mut self, flush_enabled: bool) -> Self {
        self.flush_enabled = Some(flush_enabled);
        self
    }

    pub fn ram_quota_mb(mut self, ram_quota_mb: u64) -> Self {
        self.ram_quota_mb = ram_quota_mb;
        self
    }

    pub fn num_replicas(mut self, replica_number: u32) -> Self {
        self.num_replicas = Some(replica_number);
        self
    }

    pub fn eviction_policy(mut self, eviction_policy: impl Into<EvictionPolicyType>) -> Self {
        self.eviction_policy = Some(eviction_policy.into());
        self
    }

    pub fn max_expiry(mut self, max_expiry: Duration) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    pub fn compression_mode(mut self, compression_mode: impl Into<CompressionMode>) -> Self {
        self.compression_mode = Some(compression_mode.into());
        self
    }

    pub fn minimum_durability_level(
        mut self,
        durability_min_level: impl Into<DurabilityLevel>,
    ) -> Self {
        self.minimum_durability_level = Some(durability_min_level.into());
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

    pub fn history_retention_duration(mut self, history_retention_duration: Duration) -> Self {
        self.history_retention_duration = Some(history_retention_duration);
        self
    }

    pub fn conflict_resolution_type(
        mut self,
        conflict_resolution_type: impl Into<ConflictResolutionType>,
    ) -> Self {
        self.conflict_resolution_type = Some(conflict_resolution_type.into());
        self
    }

    pub fn replica_indexes(mut self, replica_indexes: bool) -> Self {
        self.replica_indexes = Some(replica_indexes);
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

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct BucketType(InnerBucketType);

impl BucketType {
    pub const COUCHBASE: BucketType = BucketType(InnerBucketType::Couchbase);

    pub const EPHEMERAL: BucketType = BucketType(InnerBucketType::Ephemeral);

    pub(crate) fn unknown(val: String) -> BucketType {
        BucketType(InnerBucketType::Unknown(val))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerBucketType {
    Couchbase,
    Ephemeral,
    Unknown(String),
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

    pub(crate) fn unknown(val: String) -> EvictionPolicyType {
        EvictionPolicyType(InnerEvictionPolicyType::Unknown(val))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerEvictionPolicyType {
    ValueOnly,
    Full,
    NotRecentlyUsed,
    NoEviction,
    Unknown(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CompressionMode(InnerCompressionMode);

impl CompressionMode {
    pub const OFF: CompressionMode = CompressionMode(InnerCompressionMode::Off);

    pub const PASSIVE: CompressionMode = CompressionMode(InnerCompressionMode::Passive);

    pub const ACTIVE: CompressionMode = CompressionMode(InnerCompressionMode::Active);

    pub(crate) fn unknown(val: String) -> CompressionMode {
        CompressionMode(InnerCompressionMode::Unknown(val))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerCompressionMode {
    Off,
    Passive,
    Active,
    Unknown(String),
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

    pub(crate) fn unknown(val: String) -> ConflictResolutionType {
        ConflictResolutionType(InnerConflictResolutionType::Unknown(val))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerConflictResolutionType {
    SequenceNumber,
    Timestamp,
    Custom,
    Unknown(String),
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct StorageBackend(InnerStorageBackend);

impl StorageBackend {
    pub const COUCHSTORE: StorageBackend = StorageBackend(InnerStorageBackend::Couchstore);

    pub const MAGMA: StorageBackend = StorageBackend(InnerStorageBackend::Magma);

    pub(crate) fn unknown(val: String) -> StorageBackend {
        StorageBackend(InnerStorageBackend::Unknown(val))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerStorageBackend {
    Couchstore,
    Magma,
    Unknown(String),
}

impl From<BucketType> for couchbase_core::mgmtx::bucket_settings::BucketType {
    fn from(value: BucketType) -> Self {
        match value {
            BucketType::COUCHBASE => couchbase_core::mgmtx::bucket_settings::BucketType::COUCHBASE,
            BucketType::EPHEMERAL => couchbase_core::mgmtx::bucket_settings::BucketType::EPHEMERAL,
            _ => unreachable!(),
        }
    }
}

impl From<EvictionPolicyType> for couchbase_core::mgmtx::bucket_settings::EvictionPolicyType {
    fn from(value: EvictionPolicyType) -> Self {
        match value {
            EvictionPolicyType::VALUE_ONLY => {
                couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::VALUE_ONLY
            }
            EvictionPolicyType::FULL => {
                couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::FULL
            }
            EvictionPolicyType::NOT_RECENTLY_USED => {
                couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::NOT_RECENTLY_USED
            }
            EvictionPolicyType::NO_EVICTION => {
                couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::NO_EVICTION
            }
            _ => unreachable!(),
        }
    }
}

impl From<CompressionMode> for couchbase_core::mgmtx::bucket_settings::CompressionMode {
    fn from(value: CompressionMode) -> Self {
        match value {
            CompressionMode::OFF => couchbase_core::mgmtx::bucket_settings::CompressionMode::OFF,
            CompressionMode::PASSIVE => {
                couchbase_core::mgmtx::bucket_settings::CompressionMode::PASSIVE
            }
            CompressionMode::ACTIVE => {
                couchbase_core::mgmtx::bucket_settings::CompressionMode::ACTIVE
            }
            _ => unreachable!(),
        }
    }
}

impl From<ConflictResolutionType>
    for couchbase_core::mgmtx::bucket_settings::ConflictResolutionType
{
    fn from(value: ConflictResolutionType) -> Self {
        match value {
            ConflictResolutionType::SEQUENCE_NUMBER => {
                couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::SEQUENCE_NUMBER
            }
            ConflictResolutionType::TIMESTAMP => {
                couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::TIMESTAMP
            }
            ConflictResolutionType::CUSTOM => {
                couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::CUSTOM
            }
            _ => unreachable!(),
        }
    }
}

impl From<StorageBackend> for couchbase_core::mgmtx::bucket_settings::StorageBackend {
    fn from(value: StorageBackend) -> Self {
        match value {
            StorageBackend::COUCHSTORE => {
                couchbase_core::mgmtx::bucket_settings::StorageBackend::COUCHSTORE
            }
            StorageBackend::MAGMA => couchbase_core::mgmtx::bucket_settings::StorageBackend::MAGMA,
            _ => unreachable!(),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::BucketType> for BucketType {
    fn from(value: couchbase_core::mgmtx::bucket_settings::BucketType) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::BucketType::COUCHBASE => BucketType::COUCHBASE,
            couchbase_core::mgmtx::bucket_settings::BucketType::EPHEMERAL => BucketType::EPHEMERAL,
            _ => BucketType::unknown(value.to_string()),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::EvictionPolicyType> for EvictionPolicyType {
    fn from(value: couchbase_core::mgmtx::bucket_settings::EvictionPolicyType) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::VALUE_ONLY => {
                EvictionPolicyType::VALUE_ONLY
            }
            couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::FULL => {
                EvictionPolicyType::FULL
            }
            couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::NOT_RECENTLY_USED => {
                EvictionPolicyType::NOT_RECENTLY_USED
            }
            couchbase_core::mgmtx::bucket_settings::EvictionPolicyType::NO_EVICTION => {
                EvictionPolicyType::NO_EVICTION
            }
            _ => EvictionPolicyType::unknown(value.to_string()),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::CompressionMode> for CompressionMode {
    fn from(value: couchbase_core::mgmtx::bucket_settings::CompressionMode) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::CompressionMode::OFF => CompressionMode::OFF,
            couchbase_core::mgmtx::bucket_settings::CompressionMode::PASSIVE => {
                CompressionMode::PASSIVE
            }
            couchbase_core::mgmtx::bucket_settings::CompressionMode::ACTIVE => {
                CompressionMode::ACTIVE
            }
            _ => CompressionMode::unknown(value.to_string()),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::ConflictResolutionType>
    for ConflictResolutionType
{
    fn from(value: couchbase_core::mgmtx::bucket_settings::ConflictResolutionType) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::SEQUENCE_NUMBER => {
                ConflictResolutionType::SEQUENCE_NUMBER
            }
            couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::TIMESTAMP => {
                ConflictResolutionType::TIMESTAMP
            }
            couchbase_core::mgmtx::bucket_settings::ConflictResolutionType::CUSTOM => {
                ConflictResolutionType::CUSTOM
            }
            _ => ConflictResolutionType::unknown(value.to_string()),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::StorageBackend> for StorageBackend {
    fn from(value: couchbase_core::mgmtx::bucket_settings::StorageBackend) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::StorageBackend::COUCHSTORE => {
                StorageBackend::COUCHSTORE
            }
            couchbase_core::mgmtx::bucket_settings::StorageBackend::MAGMA => StorageBackend::MAGMA,
            _ => StorageBackend::unknown(value.to_string()),
        }
    }
}

impl From<BucketDef> for BucketSettings {
    fn from(value: BucketDef) -> Self {
        Self {
            name: value.name,
            ram_quota_mb: value.bucket_settings.ram_quota_mb.unwrap_or(0),
            flush_enabled: value.bucket_settings.flush_enabled,
            num_replicas: value.bucket_settings.replica_number,
            eviction_policy: value.bucket_settings.eviction_policy.map(|v| v.into()),
            max_expiry: value.bucket_settings.max_ttl,
            compression_mode: value.bucket_settings.compression_mode.map(|v| v.into()),
            minimum_durability_level: value.bucket_settings.durability_min_level.map(|v| v.into()),
            history_retention_collection_default: value
                .bucket_settings
                .history_retention_collection_default,
            history_retention_bytes: value.bucket_settings.history_retention_bytes,
            history_retention_duration: value
                .bucket_settings
                .history_retention_seconds
                .map(|s| Duration::from_secs(s as u64)),
            conflict_resolution_type: value
                .bucket_settings
                .conflict_resolution_type
                .map(|v| v.into()),
            replica_indexes: value.bucket_settings.replica_index,
            bucket_type: value.bucket_settings.bucket_type.map(|v| v.into()),
            storage_backend: value.bucket_settings.storage_backend.map(|v| v.into()),
            num_vbuckets: value.bucket_settings.num_vbuckets,
        }
    }
}
