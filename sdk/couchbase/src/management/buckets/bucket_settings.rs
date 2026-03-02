/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::durability_level::DurabilityLevel;
use couchbase_core::mgmtx::bucket_settings::BucketDef;
use std::time::Duration;

/// Settings for a Couchbase bucket.
///
/// Used with [`BucketManager::create_bucket`](super::bucket_manager::BucketManager::create_bucket)
/// and [`BucketManager::update_bucket`](super::bucket_manager::BucketManager::update_bucket).
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct BucketSettings {
    /// The name of the bucket.
    pub name: String,
    /// RAM quota in megabytes.
    pub ram_quota_mb: Option<u64>,
    /// Whether flush is enabled.
    pub flush_enabled: Option<bool>,
    /// Number of replicas.
    pub num_replicas: Option<u32>,
    /// The eviction policy for the bucket.
    pub eviction_policy: Option<EvictionPolicyType>,
    /// Maximum expiry (TTL) for documents.
    pub max_expiry: Option<Duration>,
    /// Compression mode.
    pub compression_mode: Option<CompressionMode>,
    /// Minimum durability level for writes.
    pub minimum_durability_level: Option<DurabilityLevel>,
    /// Whether history retention is enabled by default for new collections.
    pub history_retention_collection_default: Option<bool>,
    /// History retention size limit in bytes.
    pub history_retention_bytes: Option<u64>,
    /// History retention time limit.
    pub history_retention_duration: Option<Duration>,
    /// Conflict resolution strategy.
    pub conflict_resolution_type: Option<ConflictResolutionType>,
    /// Whether replica indexes are enabled.
    pub replica_indexes: Option<bool>,
    /// The type of the bucket.
    pub bucket_type: Option<BucketType>,
    /// The storage backend.
    pub storage_backend: Option<StorageBackend>,
    /// Number of vBuckets.
    pub num_vbuckets: Option<u16>,
}

impl BucketSettings {
    /// Creates a new `BucketSettings` with the given name and default values.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ram_quota_mb: None,
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

    /// Enables or disables flush.
    pub fn flush_enabled(mut self, flush_enabled: bool) -> Self {
        self.flush_enabled = Some(flush_enabled);
        self
    }

    /// Sets the RAM quota in megabytes.
    pub fn ram_quota_mb(mut self, ram_quota_mb: u64) -> Self {
        self.ram_quota_mb = Some(ram_quota_mb);
        self
    }

    /// Sets the number of replicas.
    pub fn num_replicas(mut self, replica_number: u32) -> Self {
        self.num_replicas = Some(replica_number);
        self
    }

    /// Sets the eviction policy.
    pub fn eviction_policy(mut self, eviction_policy: impl Into<EvictionPolicyType>) -> Self {
        self.eviction_policy = Some(eviction_policy.into());
        self
    }

    /// Sets the maximum document expiry.
    pub fn max_expiry(mut self, max_expiry: Duration) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    /// Sets the compression mode.
    pub fn compression_mode(mut self, compression_mode: impl Into<CompressionMode>) -> Self {
        self.compression_mode = Some(compression_mode.into());
        self
    }

    /// Sets the minimum durability level for writes.
    pub fn minimum_durability_level(
        mut self,
        durability_min_level: impl Into<DurabilityLevel>,
    ) -> Self {
        self.minimum_durability_level = Some(durability_min_level.into());
        self
    }

    /// Sets whether history retention is enabled by default for new collections.
    pub fn history_retention_collection_default(
        mut self,
        history_retention_collection_default: bool,
    ) -> Self {
        self.history_retention_collection_default = Some(history_retention_collection_default);
        self
    }

    /// Sets the history retention size limit in bytes.
    pub fn history_retention_bytes(mut self, history_retention_bytes: u64) -> Self {
        self.history_retention_bytes = Some(history_retention_bytes);
        self
    }

    /// Sets the history retention time limit.
    pub fn history_retention_duration(mut self, history_retention_duration: Duration) -> Self {
        self.history_retention_duration = Some(history_retention_duration);
        self
    }

    /// Sets the conflict resolution type.
    pub fn conflict_resolution_type(
        mut self,
        conflict_resolution_type: impl Into<ConflictResolutionType>,
    ) -> Self {
        self.conflict_resolution_type = Some(conflict_resolution_type.into());
        self
    }

    /// Enables or disables replica indexes.
    pub fn replica_indexes(mut self, replica_indexes: bool) -> Self {
        self.replica_indexes = Some(replica_indexes);
        self
    }

    /// Sets the bucket type.
    pub fn bucket_type(mut self, bucket_type: impl Into<BucketType>) -> Self {
        self.bucket_type = Some(bucket_type.into());
        self
    }

    /// Sets the storage backend.
    pub fn storage_backend(mut self, storage_backend: impl Into<StorageBackend>) -> Self {
        self.storage_backend = Some(storage_backend.into());
        self
    }

    /// Sets the number of vBuckets.
    pub fn num_vbuckets(mut self, num_vbuckets: u16) -> Self {
        self.num_vbuckets = Some(num_vbuckets);
        self
    }
}

/// The type of a Couchbase bucket.
///
/// | Constant | Description |
/// |----------|-------------|
/// | [`COUCHBASE`](BucketType::COUCHBASE) | Persistent bucket with disk storage |
/// | [`EPHEMERAL`](BucketType::EPHEMERAL) | In-memory only bucket |
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct BucketType(InnerBucketType);

impl BucketType {
    /// Persistent bucket with disk storage.
    pub const COUCHBASE: BucketType = BucketType(InnerBucketType::Couchbase);
    /// In-memory only bucket (no disk persistence).
    pub const EPHEMERAL: BucketType = BucketType(InnerBucketType::Ephemeral);
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerBucketType {
    Couchbase,
    Ephemeral,
    Unknown(String),
}

/// The eviction policy for a bucket.
///
/// | Constant | Description |
/// |----------|-------------|
/// | [`VALUE_ONLY`](EvictionPolicyType::VALUE_ONLY) | Only values are evicted (keys kept in memory) |
/// | [`FULL`](EvictionPolicyType::FULL) | Both keys and values can be evicted |
/// | [`NOT_RECENTLY_USED`](EvictionPolicyType::NOT_RECENTLY_USED) | NRU eviction (ephemeral buckets) |
/// | [`NO_EVICTION`](EvictionPolicyType::NO_EVICTION) | No eviction — operations fail when memory is full (ephemeral buckets) |
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct EvictionPolicyType(InnerEvictionPolicyType);

impl EvictionPolicyType {
    /// Only values are evicted; keys are kept in memory.
    pub const VALUE_ONLY: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::ValueOnly);
    /// Both keys and values can be evicted.
    pub const FULL: EvictionPolicyType = EvictionPolicyType(InnerEvictionPolicyType::Full);
    /// Not-recently-used eviction (for ephemeral buckets).
    pub const NOT_RECENTLY_USED: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::NotRecentlyUsed);
    /// No eviction; operations fail when memory is full (for ephemeral buckets).
    pub const NO_EVICTION: EvictionPolicyType =
        EvictionPolicyType(InnerEvictionPolicyType::NoEviction);
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerEvictionPolicyType {
    ValueOnly,
    Full,
    NotRecentlyUsed,
    NoEviction,
    Unknown(String),
}

/// The compression mode for a bucket.
///
/// | Constant | Description |
/// |----------|-------------|
/// | [`OFF`](CompressionMode::OFF) | Compression disabled |
/// | [`PASSIVE`](CompressionMode::PASSIVE) | Compressed data is stored as-is, uncompressed data is not compressed |
/// | [`ACTIVE`](CompressionMode::ACTIVE) | The server actively compresses data |
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct CompressionMode(InnerCompressionMode);

impl CompressionMode {
    /// Compression disabled.
    pub const OFF: CompressionMode = CompressionMode(InnerCompressionMode::Off);
    /// Compressed data is stored as-is; uncompressed data is not compressed.
    pub const PASSIVE: CompressionMode = CompressionMode(InnerCompressionMode::Passive);
    /// The server actively compresses data.
    pub const ACTIVE: CompressionMode = CompressionMode(InnerCompressionMode::Active);
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerCompressionMode {
    Off,
    Passive,
    Active,
    Unknown(String),
}

/// The conflict resolution type for a bucket.
///
/// | Constant | Description |
/// |----------|-------------|
/// | [`SEQUENCE_NUMBER`](ConflictResolutionType::SEQUENCE_NUMBER) | Highest sequence number wins |
/// | [`TIMESTAMP`](ConflictResolutionType::TIMESTAMP) | Highest timestamp wins |
/// | [`CUSTOM`](ConflictResolutionType::CUSTOM) | Custom conflict resolution |
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct ConflictResolutionType(InnerConflictResolutionType);

impl ConflictResolutionType {
    /// Resolve conflicts using the highest sequence number.
    pub const SEQUENCE_NUMBER: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::SequenceNumber);
    /// Resolve conflicts using the highest timestamp.
    pub const TIMESTAMP: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::Timestamp);
    /// Custom conflict resolution.
    pub const CUSTOM: ConflictResolutionType =
        ConflictResolutionType(InnerConflictResolutionType::Custom);
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerConflictResolutionType {
    SequenceNumber,
    Timestamp,
    Custom,
    Unknown(String),
}

/// The storage backend for a bucket.
///
/// | Constant | Description |
/// |----------|-------------|
/// | [`COUCHSTORE`](StorageBackend::COUCHSTORE) | Couchstore storage engine |
/// | [`MAGMA`](StorageBackend::MAGMA) | Magma storage engine (for large datasets) |
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct StorageBackend(InnerStorageBackend);

impl StorageBackend {
    /// Couchstore storage engine.
    pub const COUCHSTORE: StorageBackend = StorageBackend(InnerStorageBackend::Couchstore);
    /// Magma storage engine (optimized for large datasets).
    pub const MAGMA: StorageBackend = StorageBackend(InnerStorageBackend::Magma);
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
            _ => BucketType(InnerBucketType::Unknown(value.to_string())),
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
            _ => EvictionPolicyType(InnerEvictionPolicyType::Unknown(value.to_string())),
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
            _ => CompressionMode(InnerCompressionMode::Unknown(value.to_string())),
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
            _ => ConflictResolutionType(InnerConflictResolutionType::Unknown(value.to_string())),
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
            _ => StorageBackend(InnerStorageBackend::Unknown(value.to_string())),
        }
    }
}

impl From<BucketDef> for BucketSettings {
    fn from(value: BucketDef) -> Self {
        Self {
            name: value.name,
            ram_quota_mb: value.bucket_settings.ram_quota_mb,
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
