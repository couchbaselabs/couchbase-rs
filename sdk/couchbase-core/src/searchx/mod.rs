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

pub mod document_analysis;
pub mod ensure_index_helper;
pub mod error;
pub mod facets;
pub mod index;
mod index_json;
pub mod mgmt_options;
pub mod queries;
pub mod query_options;
pub mod search;
mod search_json;
pub mod search_respreader;
pub mod search_result;
pub mod sort;
