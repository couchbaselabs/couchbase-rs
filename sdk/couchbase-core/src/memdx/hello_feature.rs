#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum HelloFeature {
    DataType,
    Tls,
    TCPNoDelay,
    SeqNo,
    TCPDelay,
    Xattr,
    Xerror,
    SelectBucket,
    Snappy,
    Json,
    Duplex,
    ClusterMapNotif,
    UnorderedExec,
    Durations,
    AltRequests,
    SyncReplication,
    Collections,
    Opentracing,
    PreserveExpiry,
    PointInTimeRecovery,
    CreateAsDeleted,
    ReplaceBodyWithXattr,
    Unknown(u16),
}

impl From<HelloFeature> for u16 {
    fn from(value: HelloFeature) -> u16 {
        match value {
            HelloFeature::DataType => 0x01,
            HelloFeature::Tls => 0x02,
            HelloFeature::TCPNoDelay => 0x03,
            HelloFeature::SeqNo => 0x04,
            HelloFeature::TCPDelay => 0x05,
            HelloFeature::Xattr => 0x06,
            HelloFeature::Xerror => 0x07,
            HelloFeature::SelectBucket => 0x08,
            HelloFeature::Snappy => 0x0a,
            HelloFeature::Json => 0x0b,
            HelloFeature::Duplex => 0x0c,
            HelloFeature::ClusterMapNotif => 0x0d,
            HelloFeature::UnorderedExec => 0x0e,
            HelloFeature::Durations => 0x0f,
            HelloFeature::AltRequests => 0x10,
            HelloFeature::SyncReplication => 0x11,
            HelloFeature::Collections => 0x12,
            HelloFeature::Opentracing => 0x13,
            HelloFeature::PreserveExpiry => 0x14,
            HelloFeature::PointInTimeRecovery => 0x16,
            HelloFeature::CreateAsDeleted => 0x17,
            HelloFeature::ReplaceBodyWithXattr => 0x19,
            HelloFeature::Unknown(code) => code,
        }
    }
}

impl From<u16> for HelloFeature {
    fn from(value: u16) -> Self {
        match value {
            0x01 => HelloFeature::DataType,
            0x02 => HelloFeature::Tls,
            0x03 => HelloFeature::TCPNoDelay,
            0x04 => HelloFeature::SeqNo,
            0x05 => HelloFeature::TCPDelay,
            0x06 => HelloFeature::Xattr,
            0x07 => HelloFeature::Xerror,
            0x08 => HelloFeature::SelectBucket,
            0x0a => HelloFeature::Snappy,
            0x0b => HelloFeature::Json,
            0x0c => HelloFeature::Duplex,
            0x0d => HelloFeature::ClusterMapNotif,
            0x0e => HelloFeature::UnorderedExec,
            0x0f => HelloFeature::Durations,
            0x10 => HelloFeature::AltRequests,
            0x11 => HelloFeature::SyncReplication,
            0x12 => HelloFeature::Collections,
            0x13 => HelloFeature::Opentracing,
            0x14 => HelloFeature::PreserveExpiry,
            0x16 => HelloFeature::PointInTimeRecovery,
            0x17 => HelloFeature::CreateAsDeleted,
            0x19 => HelloFeature::ReplaceBodyWithXattr,
            code => HelloFeature::Unknown(code),
        }
    }
}
