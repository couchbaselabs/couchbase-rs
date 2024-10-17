use crate::collection::Collection;
use crate::results::get_result::GetResult;
use crate::transcoder::{DefaultTranscoder, Transcoder};
use serde::Serialize;

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        self.upsert_with_transcoder(id.into(), value, &DefaultTranscoder {})
            .await
    }

    pub async fn upsert_with_transcoder<T: Transcoder, V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
        transcoder: &T,
    ) -> crate::error::Result<()> {
        let (value, flags) = transcoder.encode(value)?;
        self.core_kv_client.upsert(id.into(), value, flags).await
    }

    pub async fn get(&self, id: impl Into<String>) -> crate::error::Result<GetResult> {
        self.core_kv_client.get(id.into()).await
    }
}
