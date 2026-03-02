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

use couchbase_core::queryx;

/// Describes a query (SQL++) index on the cluster.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryIndex {
    pub(crate) name: String,
    pub(crate) is_primary: bool,
    pub(crate) index_type: QueryIndexType,
    pub(crate) state: String,
    pub(crate) keyspace: String,
    pub(crate) index_key: Vec<String>,
    pub(crate) condition: Option<String>,
    pub(crate) partition: Option<String>,
    pub(crate) bucket_name: String,
    pub(crate) scope_name: Option<String>,
    pub(crate) collection_name: Option<String>,
}

impl QueryIndex {
    /// Returns the name of the index.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether this is a primary index.
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    /// Returns the type of the index (GSI or View).
    pub fn index_type(&self) -> &QueryIndexType {
        &self.index_type
    }

    /// Returns the current state of the index (e.g. "online", "deferred", "building").
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the keyspace (bucket.scope.collection) the index covers.
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Returns the index key expressions.
    pub fn index_key(&self) -> &[String] {
        &self.index_key
    }

    /// Returns the WHERE clause condition, if any.
    pub fn condition(&self) -> Option<&String> {
        self.condition.as_ref()
    }

    /// Returns the partition expression, if any.
    pub fn partition(&self) -> Option<&String> {
        self.partition.as_ref()
    }

    /// Returns the bucket name the index belongs to.
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    /// Returns the scope name, if applicable.
    pub fn scope_name(&self) -> Option<&String> {
        self.scope_name.as_ref()
    }

    /// Returns the collection name, if applicable.
    pub fn collection_name(&self) -> Option<&String> {
        self.collection_name.as_ref()
    }
}

/// The type of a query index.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum QueryIndexType {
    /// Unknown or unrecognized index type.
    Unknown,
    /// A View-based index.
    View,
    /// A Global Secondary Index.
    Gsi,
}

impl From<&str> for QueryIndexType {
    fn from(s: &str) -> Self {
        match s {
            "view" => QueryIndexType::View,
            "gsi" => QueryIndexType::Gsi,
            _ => QueryIndexType::Unknown,
        }
    }
}

impl From<queryx::index::Index> for QueryIndex {
    fn from(index: queryx::index::Index) -> Self {
        let (bucket_name, scope_name, collection_name, keyspace) =
            if let Some(bucket_id) = index.bucket_id {
                // Collections are in use so keyspace is the collection name
                (
                    bucket_id,
                    index.scope_id,
                    index.keyspace_id.clone(),
                    index.keyspace_id.unwrap_or_default(),
                )
            } else {
                // Collections are not in use so keyspace is the bucket name
                let keyspace = index.keyspace_id.clone().unwrap_or_default();
                (keyspace.clone(), None, None, keyspace)
            };

        QueryIndex {
            name: index.name,
            is_primary: index.is_primary.unwrap_or(false),
            index_type: QueryIndexType::from(index.using.as_str()),
            state: index.state,
            keyspace,
            index_key: index.index_key.unwrap_or_default(),
            condition: index.condition,
            partition: index.partition,
            bucket_name,
            scope_name,
            collection_name,
        }
    }
}
