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

//! Sub-document operation specifications.
//!
//! Sub-document operations allow you to read or mutate specific paths within a JSON document
//! without transferring the entire document. This is more efficient for large documents when
//! you only need to access a few fields.
//!
//! # Modules
//!
//! - [`lookup_in_specs`] — Specifications for sub-document lookups (get, exists, count).
//! - [`mutate_in_specs`] — Specifications for sub-document mutations (insert, upsert, replace,
//!   remove, array operations, counters).
//! - [`macros`] — Server-side macro constants for extended attributes (xattrs).

pub mod lookup_in_specs;
pub mod macros;
pub mod mutate_in_specs;
