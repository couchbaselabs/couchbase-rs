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

use crate::mgmtx::bucket_settings::{
    BucketType, CompressionMode, ConflictResolutionType, DurabilityLevel, EvictionPolicyType,
    StorageBackend,
};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct BucketSettingsJson {
    pub name: String,
    #[serde(default)]
    pub controllers: Option<Controllers>,
    #[serde(default, rename = "replicaIndex")]
    pub replica_index: Option<bool>,
    pub quota: Quota,
    #[serde(default, rename = "replicaNumber")]
    pub replica_number: Option<u32>,
    #[serde(
        deserialize_with = "deserialize_bucket_type",
        rename = "bucketType",
        default
    )]
    pub bucket_type: Option<BucketType>,
    #[serde(
        deserialize_with = "deserialize_conflict_resolution_type",
        rename = "conflictResolutionType",
        default
    )]
    pub conflict_resolution_type: Option<ConflictResolutionType>,
    #[serde(
        deserialize_with = "deserialize_eviction_policy",
        rename = "evictionPolicy",
        default
    )]
    pub eviction_policy: Option<EvictionPolicyType>,
    #[serde(default, rename = "maxTTL")]
    pub max_ttl: Option<u32>,
    #[serde(
        deserialize_with = "deserialize_compression_mode",
        rename = "compressionMode",
        default
    )]
    pub compression_mode: Option<CompressionMode>,
    #[serde(
        deserialize_with = "deserialize_durability_level",
        rename = "durabilityMinLevel",
        default
    )]
    pub durability_min_level: Option<DurabilityLevel>,
    #[serde(
        deserialize_with = "deserialize_storage_backend",
        rename = "storageBackend",
        default
    )]
    pub storage_backend: Option<StorageBackend>,
    #[serde(default, rename = "historyRetentionCollectionDefault")]
    pub history_retention_collection_default: Option<bool>,
    #[serde(default, rename = "historyRetentionBytes")]
    pub history_retention_bytes: Option<u64>,
    #[serde(default, rename = "historyRetentionSeconds")]
    pub history_retention_seconds: Option<u32>,
    #[serde(default, rename = "numVBuckets")]
    pub num_vbuckets: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct Controllers {
    pub flush: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Quota {
    pub ram: u64,
    #[serde(rename = "rawRAM")]
    pub raw_ram: u64,
}

fn deserialize_bucket_type<'de, D>(deserializer: D) -> Result<Option<BucketType>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("membase") => Ok(Some(BucketType::COUCHBASE)),
        Some("ephemeral") => Ok(Some(BucketType::EPHEMERAL)),
        Some(other) => Ok(Some(BucketType::other(other.to_string()))),
        None => Ok(None),
    }
}

fn deserialize_conflict_resolution_type<'de, D>(
    deserializer: D,
) -> Result<Option<ConflictResolutionType>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("seqno") => Ok(Some(ConflictResolutionType::SEQUENCE_NUMBER)),
        Some("lww") => Ok(Some(ConflictResolutionType::TIMESTAMP)),
        Some("custom") => Ok(Some(ConflictResolutionType::CUSTOM)),
        Some(other) => Ok(Some(ConflictResolutionType::other(other.to_string()))),
        None => Ok(None),
    }
}

fn deserialize_eviction_policy<'de, D>(
    deserializer: D,
) -> Result<Option<EvictionPolicyType>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("valueOnly") => Ok(Some(EvictionPolicyType::VALUE_ONLY)),
        Some("fullEviction") => Ok(Some(EvictionPolicyType::FULL)),
        Some("nruEviction") => Ok(Some(EvictionPolicyType::NOT_RECENTLY_USED)),
        Some("noEviction") => Ok(Some(EvictionPolicyType::NO_EVICTION)),
        Some(other) => Ok(Some(EvictionPolicyType::other(other.to_string()))),
        None => Ok(None),
    }
}

fn deserialize_compression_mode<'de, D>(
    deserializer: D,
) -> Result<Option<CompressionMode>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("off") => Ok(Some(CompressionMode::OFF)),
        Some("passive") => Ok(Some(CompressionMode::PASSIVE)),
        Some("active") => Ok(Some(CompressionMode::ACTIVE)),
        Some(other) => Ok(Some(CompressionMode::other(other.to_string()))),
        None => Ok(None),
    }
}

fn deserialize_durability_level<'de, D>(
    deserializer: D,
) -> Result<Option<DurabilityLevel>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("none") => Ok(Some(DurabilityLevel::NONE)),
        Some("majority") => Ok(Some(DurabilityLevel::MAJORITY)),
        Some("majorityAndPersistActive") => Ok(Some(DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE)),
        Some("persistToMajority") => Ok(Some(DurabilityLevel::PERSIST_TO_MAJORITY)),
        Some(other) => Ok(Some(DurabilityLevel::other(other.to_string()))),
        None => Ok(None),
    }
}

fn deserialize_storage_backend<'de, D>(deserializer: D) -> Result<Option<StorageBackend>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s.as_deref() {
        Some("couchstore") => Ok(Some(StorageBackend::COUCHSTORE)),
        Some("magma") => Ok(Some(StorageBackend::MAGMA)),
        Some(other) => Ok(Some(StorageBackend::other(other.to_string()))),
        None => Ok(None),
    }
}
