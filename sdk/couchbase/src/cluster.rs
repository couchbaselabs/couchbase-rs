use crate::bucket::Bucket;
use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::query_client::QueryClient;
use crate::clients::search_client::SearchClient;
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::buckets::bucket_manager::BucketManager;
use crate::management::users::user_manager::UserManager;
use crate::options::cluster_options::ClusterOptions;
use crate::options::query_options::QueryOptions;
use crate::options::search_options::SearchOptions;
use crate::results::query_results::QueryResult;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use std::sync::Arc;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    bucket_mgmt_client: Arc<BucketMgmtClient>,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    user_mgmt_client: Arc<UserMgmtClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let bucket_mgmt_client = Arc::new(client.buckets_client());
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let user_mgmt_client = Arc::new(client.users_client());

        Ok(Cluster {
            client,
            bucket_mgmt_client,
            query_client,
            search_client,
            user_mgmt_client,
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

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.query_client.query(statement.into(), opts.into()).await
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_client
            .search(index_name.into(), request, opts.into())
            .await
    }
}
