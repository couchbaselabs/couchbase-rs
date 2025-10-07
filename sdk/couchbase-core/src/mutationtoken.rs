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

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub(crate) vbid: u16,
    pub(crate) vbuuid: u64,
    pub(crate) seqno: u64,
}

impl MutationToken {
    pub fn new(vbid: u16, vbuuid: u64, seqno: u64) -> Self {
        Self {
            vbid,
            vbuuid,
            seqno,
        }
    }

    pub fn vbid(&self) -> u16 {
        self.vbid
    }

    pub fn vbuuid(&self) -> u64 {
        self.vbuuid
    }

    pub fn seqno(&self) -> u64 {
        self.seqno
    }

    pub fn set_vbid(&mut self, vbid: u16) {
        self.vbid = vbid;
    }

    pub fn set_vbuuid(&mut self, vbuuid: u64) {
        self.vbuuid = vbuuid;
    }

    pub fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno;
    }
}
