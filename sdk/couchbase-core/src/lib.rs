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
extern crate lazy_static;

pub mod address;
pub mod agent;
pub mod agent_ops;
pub mod auth_mechanism;
pub mod authenticator;
pub mod cbconfig;
mod collection_resolver_cached;
mod collection_resolver_memd;
mod collectionresolver;
mod compressionmanager;
mod configfetcher;
mod configmanager;
mod configparser;
mod configwatcher;
pub mod connection_state;
mod crudcomponent;
mod diagnosticscomponent;
mod errmap;
mod errmapcomponent;
pub mod error;
pub mod features;
mod helpers;
mod httpcomponent;
pub mod httpx;
mod kvclient;
mod kvclient_ops;
mod kvclientmanager;
mod kvclientpool;
pub mod memdx;
pub mod mgmtcomponent;
pub mod mgmtx;
pub mod mutationtoken;
mod networktypeheuristic;
mod nmvbhandler;
pub mod on_behalf_of;
pub mod ondemand_agentmanager;
pub mod options;
mod parsedconfig;
pub mod querycomponent;
pub mod queryx;
pub mod results;
pub mod retry;
pub mod retrybesteffort;
pub mod retryfailfast;
mod scram;
pub mod searchcomponent;
pub mod searchx;
pub mod service_type;
mod tls_config;
mod util;
mod vbucketmap;
mod vbucketrouter;

#[cfg(feature = "rustls-tls")]
pub mod insecure_certverfier;
pub mod orphan_reporter;
