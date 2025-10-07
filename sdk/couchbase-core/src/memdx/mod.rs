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

pub mod auth_mechanism;
pub mod client;
pub mod client_response;
pub mod codec;
pub mod connection;
pub mod datatype;
pub mod dispatcher;
pub mod durability_level;
pub mod error;
pub mod ext_frame_code;
pub mod extframe;
pub mod hello_feature;
pub mod magic;
pub mod op_auth_saslauto;
pub mod op_auth_saslbyname;
pub mod op_auth_saslplain;
pub mod op_auth_saslscram;
pub mod op_bootstrap;
pub mod opcode;
pub mod ops_core;
pub mod ops_crud;
pub mod ops_util;
pub mod packet;
pub mod pendingop;
pub mod request;
pub mod response;
pub mod status;
pub mod subdoc;
pub mod sync_helpers;
