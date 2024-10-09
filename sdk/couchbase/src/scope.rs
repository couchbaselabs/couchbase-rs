use crate::clients::scope_client::ScopeClient;
use crate::collection::Collection;

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        Self { client }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn collection(&self, name: impl Into<String>) -> Collection {
        Collection::new(self.client.collection_client(name.into()))
    }
}
