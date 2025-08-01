#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataTypeFlag {
    None,
    Json,
    Compressed,
    Xattrs,
}

impl Default for DataTypeFlag {
    fn default() -> Self {
        Self::None
    }
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
