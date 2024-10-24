use crate::transcoding::RawValue;
use crate::{error, transcoding};
use bytes::Bytes;
use serde::de::DeserializeOwned;

pub struct GetResult {
    content: Bytes,
    flags: u32,
}

impl GetResult {
    pub fn content_as<V: DeserializeOwned>(self) -> error::Result<V> {
        let content = self.content_as_raw();
        transcoding::json::decode(&content)
    }

    pub fn content_as_raw(self) -> RawValue {
        RawValue {
            content: self.content,
            flags: self.flags,
        }
    }
}

impl From<couchbase_core::crudresults::GetResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetResult) -> Self {
        Self {
            content: Bytes::from(result.value),
            flags: result.flags,
        }
    }
}
