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
use crate::common::test_config::{create_test_cluster, run_test};
#[cfg(feature = "unstable-jwt")]
use couchbase::authenticator::{Authenticator, JwtAuthenticator};

mod common;

#[cfg(feature = "unstable-jwt")]
#[test]
fn test_set_authenticator_different_type() {
    run_test(async |_cluster, _bucket| {
        let cluster = create_test_cluster().await;

        let err = cluster
            .set_authenticator(Authenticator::JwtAuthenticator(JwtAuthenticator::new(
                "somethingmadeup",
            )))
            .await
            .err()
            .unwrap();

        match err.kind() {
            couchbase::error::ErrorKind::InvalidArgument(_) => {}
            _ => panic!("Expected InvalidArgument error"),
        }
    })
}
