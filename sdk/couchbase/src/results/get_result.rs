use crate::error;
use crate::transcoder::{DefaultTranscoder, Transcoder};
use bytes::Bytes;
use serde::de::DeserializeOwned;

pub struct GetResult {
    content: Bytes,
    flags: u32,
}

impl GetResult {
    pub fn content_as<V: DeserializeOwned>(self) -> error::Result<V> {
        self.content_as_with_transcoder(&DefaultTranscoder {})
    }

    pub fn content_as_with_transcoder<T: Transcoder, V: DeserializeOwned>(
        &self,
        transcoder: &T,
    ) -> error::Result<V> {
        transcoder.decode(&self.content, self.flags)
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
