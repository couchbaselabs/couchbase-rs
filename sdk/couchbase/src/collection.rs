use crate::clients::collection_client::CollectionClient;
use crate::clients::core_kv_client::CoreKvClient;
use crate::clients::query_index_mgmt_client::QueryIndexMgmtClient;
use crate::clients::tracing_client::TracingClient;
use crate::query_index_manager::CollectionQueryIndexManager;
use std::sync::Arc;

#[derive(Clone)]
pub struct Collection {
    pub(crate) client: CollectionClient,
    pub(crate) core_kv_client: CoreKvClient,
    pub(crate) query_index_management_client: Arc<QueryIndexMgmtClient>,
    pub(crate) tracing_client: Arc<TracingClient>,
}

impl Collection {
    pub(crate) fn new(client: CollectionClient) -> Self {
        let core_kv_client = client.core_kv_client();
        let query_index_management_client = Arc::new(client.query_index_management_client());
        let tracing_client = Arc::new(client.tracing_client());
        Self {
            client,
            core_kv_client,
            query_index_management_client,
            tracing_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn scope_name(&self) -> &str {
        self.client.scope_name()
    }

    pub fn bucket_name(&self) -> &str {
        self.client.bucket_name()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(self.core_kv_client.clone(), self.tracing_client.clone())
    }

    pub fn query_indexes(&self) -> CollectionQueryIndexManager {
        CollectionQueryIndexManager::new(self.query_index_management_client.clone())
    }
}

#[derive(Clone)]
pub struct BinaryCollection {
    pub(crate) core_kv_client: CoreKvClient,
    pub(crate) tracing_client: Arc<TracingClient>,
}

impl BinaryCollection {
    pub(crate) fn new(core_kv_client: CoreKvClient, tracing_client: Arc<TracingClient>) -> Self {
        Self {
            core_kv_client,
            tracing_client,
        }
    }
}
