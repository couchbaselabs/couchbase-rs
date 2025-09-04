use crate::bucket::Bucket;
use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::diagnostics_client::DiagnosticsClient;
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::buckets::bucket_manager::BucketManager;
use crate::management::users::user_manager::UserManager;
use crate::options::cluster_options::ClusterOptions;
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::results::diagnostics::{DiagnosticsResult, PingReport};
use std::sync::Arc;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    bucket_mgmt_client: Arc<BucketMgmtClient>,
    user_mgmt_client: Arc<UserMgmtClient>,
    diagnostics_client: Arc<DiagnosticsClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let bucket_mgmt_client = Arc::new(client.buckets_client());
        let user_mgmt_client = Arc::new(client.users_client());
        let diagnostics_client = Arc::new(client.diagnostics_client());

        Ok(Cluster {
            client,
            bucket_mgmt_client,
            user_mgmt_client,
            diagnostics_client,
        })
    }

    pub fn bucket(&self, name: impl Into<String>) -> Bucket {
        let bucket_client = self.client.bucket_client(name.into());

        Bucket::new(bucket_client)
    }

    pub fn buckets(&self) -> BucketManager {
        BucketManager::new(self.bucket_mgmt_client.clone())
    }

    pub fn users(&self) -> UserManager {
        UserManager::new(self.user_mgmt_client.clone())
    }

    pub async fn diagnostics(
        &self,
        opts: impl Into<Option<DiagnosticsOptions>>,
    ) -> error::Result<DiagnosticsResult> {
        self.diagnostics_client.diagnostics(opts.into()).await
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
