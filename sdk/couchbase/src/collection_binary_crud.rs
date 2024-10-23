use crate::collection::BinaryCollection;
use crate::options::kv_binary_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::MutationResult;

impl BinaryCollection {
    pub async fn append(
        &self,
        id: impl Into<String>,
        value: Vec<u8>,
        options: impl Into<Option<AppendOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.append(id.into(), value, options).await
    }

    pub async fn prepend(
        &self,
        id: impl Into<String>,
        value: Vec<u8>,
        options: impl Into<Option<PrependOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.prepend(id.into(), value, options).await
    }

    pub async fn increment(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.increment(id.into(), options).await
    }

    pub async fn decrement(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.decrement(id.into(), options).await
    }
}
