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

use crate::management::collections::collection_settings::MaxExpiryValue;

/// Describes a scope and its collections, returned by collection management operations.
#[derive(Default, Debug, Clone)]
pub struct ScopeSpec {
    pub(crate) name: String,
    pub(crate) collections: Vec<CollectionSpec>,
}

impl ScopeSpec {
    /// Returns the name of the scope.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the collections within this scope.
    pub fn collections(&self) -> &[CollectionSpec] {
        &self.collections
    }
}

/// Describes a collection within a scope.
#[derive(Debug, Clone)]
pub struct CollectionSpec {
    pub(crate) name: String,
    pub(crate) scope_name: String,
    pub(crate) max_expiry: MaxExpiryValue,
    pub(crate) history: bool,
}

impl CollectionSpec {
    /// Returns the name of the collection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the scope this collection belongs to.
    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    /// Returns the maximum expiry setting for documents in this collection.
    pub fn max_expiry(&self) -> MaxExpiryValue {
        self.max_expiry
    }

    /// Returns whether history retention is enabled for this collection.
    pub fn history(&self) -> bool {
        self.history
    }
}
