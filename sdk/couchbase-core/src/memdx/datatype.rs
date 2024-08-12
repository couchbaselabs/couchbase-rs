#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DataTypeFlag {
    None,
    Json,
    Compressed,
    Xattrs,
}

impl From<DataTypeFlag> for u8 {
    fn from(value: DataTypeFlag) -> Self {
        match value {
            DataTypeFlag::None => 0,
            DataTypeFlag::Json => 1,
            DataTypeFlag::Compressed => 2,
            DataTypeFlag::Xattrs => 4,
        }
    }
}
