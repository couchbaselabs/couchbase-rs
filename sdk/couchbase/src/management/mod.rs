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

//! Cluster management APIs.
//!
//! These modules provide managers for administering various cluster resources:
//!
//! - [`buckets`] — Create, update, drop, list, and flush buckets
//!   ([`BucketManager`](buckets::bucket_manager::BucketManager))
//! - [`collections`] — Create, drop, and list scopes and collections
//!   ([`CollectionManager`](collections::collection_manager::CollectionManager))
//! - [`users`] — Create, update, drop, and list users and groups
//!   ([`UserManager`](users::user_manager::UserManager))
//! - [`query`] — Create, drop, list, and build query indexes
//!   ([`QueryIndexManager`](query::query_index_manager::QueryIndexManager))
//! - [`search`] — Create, update, drop, and list search indexes
//!   ([`SearchIndexManager`](search::search_index_manager::SearchIndexManager))

pub mod buckets;
pub mod collections;
pub mod query;
pub mod search;
pub mod users;
