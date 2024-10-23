use crate::clients::collection_client::CollectionClient;
use crate::clients::core_kv_client::CoreKvClient;

#[derive(Clone)]
pub struct Collection {
    pub(crate) client: CollectionClient,
    pub(crate) core_kv_client: CoreKvClient,
}

impl Collection {
    pub(crate) fn new(client: CollectionClient) -> Self {
        let core_kv_client = client.core_kv_client();
        Self {
            client,
            core_kv_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(self.core_kv_client.clone())
    }
}

#[derive(Clone)]
pub struct BinaryCollection {
    pub(crate) core_kv_client: CoreKvClient,
}

impl BinaryCollection {
    pub(crate) fn new(core_kv_client: CoreKvClient) -> Self {
        Self { core_kv_client }
    }
}
