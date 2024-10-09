use crate::bucket::Bucket;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::query_client::QueryClient;
use crate::error;
use crate::options::cluster_options::ClusterOptions;
use std::sync::Arc;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    query_client: Arc<QueryClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let query_client = Arc::new(client.query_client()?);

        Ok(Cluster {
            client,
            query_client,
        })
    }

    pub async fn bucket(&self, name: impl Into<String>) -> Bucket {
        // TODO: unwrap
        let bucket_client = self.client.bucket_client(name.into()).await.unwrap();

        Bucket::new(bucket_client)
    }

    pub async fn query(&self, query: impl Into<String>) -> error::Result<()> {
        self.query_client.query(query.into()).await
    }
}
