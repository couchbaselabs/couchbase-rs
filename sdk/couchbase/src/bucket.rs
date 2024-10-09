use crate::clients::bucket_client::BucketClient;
use crate::collection::Collection;
use crate::scope::Scope;

#[derive(Clone)]
pub struct Bucket {
    client: BucketClient,
}

impl Bucket {
    pub(crate) fn new(client: BucketClient) -> Self {
        Self { client }
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
}
