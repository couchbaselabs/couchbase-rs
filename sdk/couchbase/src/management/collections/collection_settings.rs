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

use std::time::Duration;

/// Controls the maximum document expiry for a collection.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum MaxExpiryValue {
    /// Documents never expire.
    Never,
    /// Inherit the expiry setting from the parent bucket.
    InheritFromBucket,
    /// Documents expire after the given duration.
    Seconds(Duration),
}

impl From<MaxExpiryValue> for i32 {
    fn from(value: MaxExpiryValue) -> Self {
        match value {
            MaxExpiryValue::Never => 0,
            MaxExpiryValue::InheritFromBucket => -1,
            MaxExpiryValue::Seconds(duration) => duration.as_secs() as i32,
        }
    }
}

impl From<i32> for MaxExpiryValue {
    fn from(value: i32) -> Self {
        match value {
            0 => MaxExpiryValue::Never,
            -1 => MaxExpiryValue::InheritFromBucket,
            _ => MaxExpiryValue::Seconds(Duration::from_secs(value as u64)),
        }
    }
}

/// Settings for creating a new collection.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionSettings {
    /// Maximum document expiry for the collection.
    pub max_expiry: Option<MaxExpiryValue>,
    /// Whether to enable history retention.
    pub history: Option<bool>,
}

impl CreateCollectionSettings {
    /// Creates a new `CreateCollectionSettings` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the maximum document expiry.
    pub fn max_expiry(mut self, max_expiry: MaxExpiryValue) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    /// Enables or disables history retention.
    pub fn history(mut self, history: bool) -> Self {
        self.history = Some(history);
        self
    }
}

/// Settings for updating an existing collection.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionSettings {
    /// Maximum document expiry for the collection.
    pub max_expiry: Option<MaxExpiryValue>,
    /// Whether to enable history retention.
    pub history: Option<bool>,
}

impl UpdateCollectionSettings {
    /// Creates a new `UpdateCollectionSettings` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the maximum document expiry.
    pub fn max_expiry(mut self, max_expiry: MaxExpiryValue) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    /// Enables or disables history retention.
    pub fn history(mut self, history: bool) -> Self {
        self.history = Some(history);
        self
    }
}
