#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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

impl Into<u16> for ExtReqFrameCode {
    fn into(self) -> u16 {
        match self {
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
pub enum ExtResFrameCode {
    ServerDuration,
    ReadUnits,
    WriteUnits,
    ThrottleDuration,

    Unknown(u16),
}

impl Into<u16> for ExtResFrameCode {
    fn into(self) -> u16 {
        match self {
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
