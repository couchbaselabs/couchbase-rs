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
}
