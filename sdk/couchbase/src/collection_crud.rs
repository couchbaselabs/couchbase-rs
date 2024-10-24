use crate::collection::Collection;
use crate::results::get_result::GetResult;
use crate::transcoding;
use crate::transcoding::RawValue;
use serde::Serialize;

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        self.upsert_raw(id.into(), transcoding::json::encode(value)?)
            .await
    }

    pub async fn upsert_raw(
        &self,
        id: impl Into<String>,
        value: RawValue,
    ) -> crate::error::Result<()> {
        self.core_kv_client
            .upsert(id.into(), value.content, value.flags)
            .await
    }

    pub async fn get(&self, id: impl Into<String>) -> crate::error::Result<GetResult> {
        self.core_kv_client.get(id.into()).await
    }
}
