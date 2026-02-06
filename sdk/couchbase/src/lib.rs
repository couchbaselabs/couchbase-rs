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

extern crate core;

#[macro_use]
mod tracing;
pub mod authenticator;
pub mod bucket;
mod capella_ca;
mod clients;
pub mod cluster;
pub mod collection;
pub mod collection_binary_crud;
pub mod collection_crud;
pub mod collection_ds;
pub mod diagnostics;
pub mod durability_level;
pub mod error;
pub mod error_context;
pub mod logging_meter;
pub mod management;
pub mod mutation_state;
pub mod options;
pub mod results;
pub mod retry;
pub mod scope;
pub mod search;
pub mod service_type;
pub mod subdoc;
pub mod threshold_log_tracer;
pub mod transcoding;
