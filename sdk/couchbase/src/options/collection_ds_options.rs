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

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseListOptions {}

impl CouchbaseListOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseMapOptions {}

impl CouchbaseMapOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseSetOptions {}

impl CouchbaseSetOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseQueueOptions {}

impl CouchbaseQueueOptions {
    pub fn new() -> Self {
        Default::default()
    }
}
