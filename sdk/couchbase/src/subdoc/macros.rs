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

use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum LookupInMacros {
    Document,
    ExpiryTime,
    CAS,
    SeqNo,
    LastModified,
    IsDeleted,
    ValueSizeBytes,
    RevId,
}

impl From<LookupInMacros> for String {
    fn from(macro_: LookupInMacros) -> Self {
        match macro_ {
            LookupInMacros::Document => "$document".to_string(),
            LookupInMacros::ExpiryTime => "$document.exptime".to_string(),
            LookupInMacros::CAS => "$document.CAS".to_string(),
            LookupInMacros::SeqNo => "$document.seqno".to_string(),
            LookupInMacros::LastModified => "$document.last_modified".to_string(),
            LookupInMacros::IsDeleted => "$document.deleted".to_string(),
            LookupInMacros::ValueSizeBytes => "$document.value_bytes".to_string(),
            LookupInMacros::RevId => "$document.revid".to_string(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum MutateInMacros {
    #[serde(rename = "${Mutation.CAS}")]
    CAS,
    #[serde(rename = "${Mutation.seqno}")]
    SeqNo,
    #[serde(rename = "${Mutation.value_crc32c}")]
    ValueCrc32c,
}

pub(crate) static MUTATE_IN_MACROS: &[&[u8]] = &[
    br#""${Mutation.CAS}""#,
    br#""${Mutation.seqno}""#,
    br#""${Mutation.value_crc32c}""#,
];
