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

use crate::common::new_key;
use crate::common::test_config::run_test;
use std::ops::Add;

mod common;

#[test]
#[should_panic]
fn test_upsert() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket("idonotexistonthiscluster")
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection
            .upsert(&key, "test", None)
            .await
            .expect("Expected panic due to timeout");
    })
}
