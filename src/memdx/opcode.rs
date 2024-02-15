use crate::memdx::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum OpCode {
    Get,
    Set,
    Add,
    Hello,
    GetErrorMap,
    SelectBucket,
    SASLAuth,
}

impl Into<u8> for OpCode {
    fn into(self) -> u8 {
        match self {
            OpCode::Get => 0x00,
            OpCode::Set => 0x01,
            OpCode::Add => 0x02,
            OpCode::Hello => 0x1f,
            OpCode::SASLAuth => 0x21,
            OpCode::SelectBucket => 0x89,
            OpCode::GetErrorMap => 0xfe,
        }
    }
}

impl TryFrom<u8> for OpCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let code = match value {
            0x00 => OpCode::Get,
            0x01 => OpCode::Set,
            0x02 => OpCode::Add,
            0x1f => OpCode::Hello,
            0x21 => OpCode::SASLAuth,
            0x89 => OpCode::SelectBucket,
            0xfe => OpCode::GetErrorMap,
            _ => {
                return Err(Error::Protocol(format!("unknown opcode {}", value)));
            }
        };

        Ok(code)
    }
}

impl Display for OpCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            OpCode::Get => "Get",
            OpCode::Set => "Set",
            OpCode::Add => "Add",
            OpCode::Hello => "Hello",
            OpCode::GetErrorMap => "Get error map",
            OpCode::SelectBucket => "Select bucket",
            OpCode::SASLAuth => "SASL auth",
        };
        write!(f, "{}", txt)
    }
}
