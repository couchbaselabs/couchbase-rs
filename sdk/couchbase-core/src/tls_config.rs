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

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use std::sync::Arc;
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
pub type TlsConfig = Arc<tokio_rustls::rustls::ClientConfig>;

#[cfg(feature = "native-tls")]
pub type TlsConfig = tokio_native_tls::native_tls::TlsConnector;

#[cfg(not(any(feature = "rustls-tls", feature = "native-tls")))]
compile_error!("At least one of the features 'rustls-tls' or 'native-tls' must be enabled.");
