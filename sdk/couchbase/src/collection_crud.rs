use crate::collection::Collection;
use serde::Serialize;

impl Collection {
    pub async fn upsert<T: Serialize>(&self, id: String, value: T) -> crate::error::Result<()> {
        self.core_kv_client.upsert(id, value).await
    }
}
