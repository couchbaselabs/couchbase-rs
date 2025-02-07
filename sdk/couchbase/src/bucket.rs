use crate::clients::bucket_client::BucketClient;
use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::collection::Collection;
use crate::collections_manager::CollectionManager;
use crate::scope::Scope;
use std::sync::Arc;

#[derive(Clone)]
pub struct Bucket {
    client: BucketClient,
    collections_mgmt_client: Arc<CollectionsMgmtClient>,
}

impl Bucket {
    pub(crate) fn new(client: BucketClient) -> Self {
        let collections_mgmt_client = Arc::new(client.collections_management_client());
        Self {
            client,
            collections_mgmt_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn scope(&self, name: impl Into<String>) -> Scope {
        Scope::new(self.client.scope_client(name.into()))
    }

    pub fn collection(&self, name: impl Into<String>) -> Collection {
        self.scope("_default").collection(name)
    }

    pub fn default_collection(&self) -> Collection {
        self.collection("_default".to_string())
    }

    pub fn collections(&self) -> CollectionManager {
        CollectionManager::new(self.collections_mgmt_client.clone())
    }
}
