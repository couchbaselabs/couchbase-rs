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

use std::time::Duration;

#[derive(Clone)]
#[non_exhaustive]
pub struct OrphanReporterConfig {
    pub reporter_interval: Duration,
    pub sample_size: usize,
}

impl OrphanReporterConfig {
    pub fn reporter_interval(mut self, reporter_interval: Duration) -> Self {
        self.reporter_interval = reporter_interval;
        self
    }

    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }
}

impl Default for OrphanReporterConfig {
    fn default() -> Self {
        Self {
            reporter_interval: Duration::from_secs(10),
            sample_size: 10,
        }
    }
}
