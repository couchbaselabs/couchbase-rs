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

pub(crate) use couchbase_core::tracingcomponent::{
    Keyspace, SpanBuilder, SERVICE_VALUE_KV, SERVICE_VALUE_MANAGEMENT, SERVICE_VALUE_QUERY,
    SERVICE_VALUE_SEARCH, SPAN_ATTRIB_CLUSTER_NAME_KEY, SPAN_ATTRIB_CLUSTER_UUID_KEY,
    SPAN_ATTRIB_DB_DURABILITY, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_LOCAL_ID_KEY,
    SPAN_ATTRIB_NET_PEER_ADDRESS_KEY, SPAN_ATTRIB_NET_PEER_PORT_KEY, SPAN_ATTRIB_OPERATION_ID_KEY,
    SPAN_ATTRIB_OPERATION_KEY, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE, SPAN_ATTRIB_SERVER_DURATION_KEY,
    SPAN_ATTRIB_SERVICE_KEY, SPAN_NAME_DISPATCH_TO_SERVER, SPAN_NAME_REQUEST_ENCODING,
};
