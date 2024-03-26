use std::fmt::Debug;

use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::status::Status;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResponsePacket {
    pub magic: Magic,
    pub op_code: OpCode,
    pub datatype: u8,
    pub status: Status,
    pub opaque: u32,
    pub vbucket_id: Option<u16>,
    pub cas: Option<u64>,
    pub extras: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub framing_extras: Option<Vec<u8>>,
}

impl ResponsePacket {
    pub fn new(magic: Magic, op_code: OpCode, datatype: u8, status: Status, opaque: u32) -> Self {
        Self {
            magic,
            op_code,
            datatype,
            status,
            opaque,
            vbucket_id: None,
            cas: None,
            extras: None,
            key: None,
            value: None,
            framing_extras: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestPacket {
    pub magic: Magic,
    pub op_code: OpCode,
    pub datatype: u8,
    pub vbucket_id: Option<u16>,
    pub cas: Option<u64>,
    pub extras: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub framing_extras: Option<Vec<u8>>,

    pub opaque: Option<u32>,
}
