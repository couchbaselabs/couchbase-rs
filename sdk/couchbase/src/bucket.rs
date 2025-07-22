use crate::clients::bucket_client::BucketClient;
use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::clients::diagnostics_client::DiagnosticsClient;
use crate::collection::Collection;
use crate::error;
use crate::management::collections::collection_manager::CollectionManager;
use crate::options::diagnostic_options::{PingOptions, WaitUntilReadyOptions};
use crate::results::diagnostics::PingReport;
use crate::scope::Scope;

#[derive(Clone)]
pub struct Bucket {
    client: BucketClient,
    collections_mgmt_client: CollectionsMgmtClient,
    diagnostics_client: DiagnosticsClient,
}

impl Bucket {
    pub(crate) fn new(client: BucketClient) -> Self {
        let collections_mgmt_client = client.collections_management_client();
        let diagnostics_client = client.diagnostics_client();

        Self {
            client,
            collections_mgmt_client,
            diagnostics_client,
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
        CollectionManager {
            client: self.collections_mgmt_client.clone(),
        }
    }

    pub async fn ping(&self, opts: impl Into<Option<PingOptions>>) -> error::Result<PingReport> {
        self.diagnostics_client.ping(opts.into()).await
    }

    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        self.diagnostics_client.wait_until_ready(opts.into()).await
    }
}
