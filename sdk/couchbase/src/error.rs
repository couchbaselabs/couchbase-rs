pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    pub msg: String,
}

impl From<couchbase_core::error::Error> for Error {
    fn from(error: couchbase_core::error::Error) -> Self {
        Self {
            msg: error.to_string(),
        }
    }
}
