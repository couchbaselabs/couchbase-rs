use std::fmt::{Display, Formatter};

use crate::memdx::error::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum OpCode {
    Get,
    Set,
    Add,
    Hello,
    GetErrorMap,
    SelectBucket,
    SASLAuth,
    SASLListMechs,
    SASLStep,
}

impl From<OpCode> for u8 {
    fn from(value: OpCode) -> Self {
        match value {
            OpCode::Get => 0x00,
            OpCode::Set => 0x01,
            OpCode::Add => 0x02,
            OpCode::Hello => 0x1f,
            OpCode::SASLListMechs => 0x20,
            OpCode::SASLAuth => 0x21,
            OpCode::SASLStep => 0x22,
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
            0x20 => OpCode::SASLListMechs,
            0x21 => OpCode::SASLAuth,
            0x22 => OpCode::SASLStep,
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
            OpCode::SASLListMechs => "SASL list mechanisms",
            OpCode::SASLStep => "SASL step",
        };
        write!(f, "{}", txt)
    }
}
