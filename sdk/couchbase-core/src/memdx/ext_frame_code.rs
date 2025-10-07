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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum ExtReqFrameCode {
    Barrier,
    Durability,
    StreamID,
    OtelContext,
    OnBehalfOf,
    PreserveTTL,
    ExtraPerm,

    Unknown(u16),
}

impl From<ExtReqFrameCode> for u16 {
    fn from(value: ExtReqFrameCode) -> u16 {
        match value {
            ExtReqFrameCode::Barrier => 0x00,
            ExtReqFrameCode::Durability => 0x01,
            ExtReqFrameCode::StreamID => 0x02,
            ExtReqFrameCode::OtelContext => 0x03,
            ExtReqFrameCode::OnBehalfOf => 0x04,
            ExtReqFrameCode::PreserveTTL => 0x05,
            ExtReqFrameCode::ExtraPerm => 0x06,

            ExtReqFrameCode::Unknown(code) => code,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum ExtResFrameCode {
    ServerDuration,
    ReadUnits,
    WriteUnits,
    ThrottleDuration,

    Unknown(u16),
}

impl From<ExtResFrameCode> for u16 {
    fn from(value: ExtResFrameCode) -> u16 {
        match value {
            ExtResFrameCode::ServerDuration => 0x00,
            ExtResFrameCode::ReadUnits => 0x01,
            ExtResFrameCode::WriteUnits => 0x02,
            ExtResFrameCode::ThrottleDuration => 0x03,

            ExtResFrameCode::Unknown(code) => code,
        }
    }
}

impl From<u16> for ExtResFrameCode {
    fn from(value: u16) -> Self {
        match value {
            0x00 => ExtResFrameCode::ServerDuration,
            0x01 => ExtResFrameCode::ReadUnits,
            0x02 => ExtResFrameCode::WriteUnits,
            0x03 => ExtResFrameCode::ThrottleDuration,
            _ => ExtResFrameCode::Unknown(value),
        }
    }
}
