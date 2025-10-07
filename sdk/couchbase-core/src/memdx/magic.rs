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

use std::fmt::{Debug, Display};

use crate::memdx::error::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Magic {
    Req,
    Res,
    ReqExt,
    ResExt,

    ServerReq,
    ServerRes,
}

impl Magic {
    pub fn is_request(&self) -> bool {
        matches!(self, Magic::Req | Magic::ReqExt)
    }

    pub fn is_response(&self) -> bool {
        matches!(self, Magic::Res | Magic::ResExt)
    }

    pub fn is_extended(&self) -> bool {
        matches!(self, Magic::ReqExt | Magic::ResExt)
    }
}

impl From<Magic> for u8 {
    fn from(value: Magic) -> u8 {
        match value {
            Magic::Req => 0x80,
            Magic::Res => 0x81,
            Magic::ReqExt => 0x08,
            Magic::ResExt => 0x18,
            Magic::ServerReq => 0x82,
            Magic::ServerRes => 0x83,
        }
    }
}

impl TryFrom<u8> for Magic {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let magic = match value {
            0x80 => Magic::Req,
            0x81 => Magic::Res,
            0x08 => Magic::ReqExt,
            0x18 => Magic::ResExt,
            0x82 => Magic::ServerReq,
            0x83 => Magic::ServerRes,
            _ => {
                return Err(Error::new_message_error(format!("unknown magic {value}")));
            }
        };

        Ok(magic)
    }
}

impl Display for Magic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            Magic::Req => "Req",
            Magic::Res => "Res",
            Magic::ReqExt => "ReqExt",
            Magic::ResExt => "ResExt",
            Magic::ServerReq => "ServerReq",
            Magic::ServerRes => "ServerRes",
        };
        write!(f, "{txt}")
    }
}
