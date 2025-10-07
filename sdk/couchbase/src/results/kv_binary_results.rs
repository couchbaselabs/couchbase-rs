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

use crate::mutation_state::MutationToken;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CounterResult {
    pub(crate) cas: u64,
    pub(crate) mutation_token: Option<MutationToken>,
    pub(crate) content: u64,
}

impl CounterResult {
    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> &Option<MutationToken> {
        &self.mutation_token
    }

    pub fn content(&self) -> u64 {
        self.content
    }
}
