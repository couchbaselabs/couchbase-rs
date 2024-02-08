use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum OpCode {
    Get,
    Set,
    Add,
}

impl Into<u8> for OpCode {
    fn into(self) -> u8 {
        match self {
            OpCode::Get => 0x00,
            OpCode::Set => 0x01,
            OpCode::Add => 0x02,
        }
    }
}

impl TryFrom<u8> for OpCode {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let code = match value {
            0x00 => OpCode::Get,
            0x01 => OpCode::Set,
            0x02 => OpCode::Add,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown opcode {}", value),
                ));
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
        };
        write!(f, "{}", txt)
    }
}
