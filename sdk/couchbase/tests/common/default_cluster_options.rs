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

use crate::common::test_config::TestSetupConfig;
use couchbase::authenticator::{Authenticator, PasswordAuthenticator};
use couchbase::options::cluster_options::{ClusterOptions, TlsOptions};

pub async fn create_default_options(config: TestSetupConfig) -> ClusterOptions {
    let mut opts = ClusterOptions::new(Authenticator::PasswordAuthenticator(
        PasswordAuthenticator {
            username: config.username.clone(),
            password: config.password.clone(),
        },
    ));

    if config.resolved_conn_spec.use_ssl {
        opts = opts.tls_options(TlsOptions::new().danger_accept_invalid_certs(true));
    };

    opts
}
