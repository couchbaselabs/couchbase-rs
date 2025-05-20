use couchbase::collection::BinaryCollection;
use couchbase::error;
use couchbase::options::kv_binary_options::*;
use couchbase::results::kv_binary_results::CounterResult;
use couchbase::results::kv_results::*;
use std::ops::Deref;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct TestBinaryCollection {
    inner: BinaryCollection,
}

impl Deref for TestBinaryCollection {
    type Target = BinaryCollection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestBinaryCollection {
    pub fn new(inner: BinaryCollection) -> Self {
        Self { inner }
    }

    pub async fn append(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.append(id, value, options),
        )
        .await
        .unwrap()
    }

    pub async fn prepend(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.prepend(id, value, options),
        )
        .await
        .unwrap()
    }

    pub async fn increment(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> error::Result<CounterResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.increment(id, options),
        )
        .await
        .unwrap()
    }

    pub async fn decrement(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> error::Result<CounterResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.decrement(id, options),
        )
        .await
        .unwrap()
    }
}
