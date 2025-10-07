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

use crate::transcoding::{decode_common_flags, encode_common_flags, DataType};
use serde::Serialize;

pub fn encode<T: Serialize>(value: T) -> crate::error::Result<(Vec<u8>, u32)> {
    let content = serde_json::to_vec(&value)
        .map_err(|e| crate::error::Error::other_failure(e.to_string()))?;
    let flags = encode_common_flags(DataType::Json);

    Ok((content, flags))
}

pub fn decode<T: serde::de::DeserializeOwned>(value: &[u8], flags: u32) -> crate::error::Result<T> {
    let datatype = decode_common_flags(flags);
    if datatype != DataType::Json {
        return Err(crate::error::Error::other_failure("datatype not supported"));
    }

    serde_json::from_slice(value).map_err(|e| crate::error::Error::other_failure(e.to_string()))
}
