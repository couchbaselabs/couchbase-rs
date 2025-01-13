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
    pub(crate) magic: Magic,
    pub(crate) op_code: OpCode,
    pub(crate) datatype: u8,
    pub(crate) vbucket_id: Option<u16>,
    pub(crate) cas: Option<u64>,
    pub(crate) extras: Option<Vec<u8>>,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) framing_extras: Option<Vec<u8>>,
    pub(crate) opaque: Option<u32>,
}

impl RequestPacket {
    pub fn new(magic: Magic, op_code: OpCode, datatype: u8) -> Self {
        Self {
            magic,
            op_code,
            datatype,
            vbucket_id: None,
            cas: None,
            extras: None,
            key: None,
            value: None,
            framing_extras: None,
            opaque: None,
        }
    }

    pub fn magic(mut self, magic: Magic) -> Self {
        self.magic = magic;
        self
    }

    pub fn op_code(mut self, op_code: OpCode) -> Self {
        self.op_code = op_code;
        self
    }

    pub fn datatype(mut self, datatype: u8) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn vbucket_id(mut self, vbucket_id: u16) -> Self {
        self.vbucket_id = Some(vbucket_id);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn extras(mut self, extras: Vec<u8>) -> Self {
        self.extras = Some(extras);
        self
    }

    pub fn key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = Some(value);
        self
    }

    pub fn framing_extras(mut self, framing_extras: Vec<u8>) -> Self {
        self.framing_extras = Some(framing_extras);
        self
    }

    pub fn opaque(mut self, opaque: u32) -> Self {
        self.opaque = Some(opaque);
        self
    }
}
