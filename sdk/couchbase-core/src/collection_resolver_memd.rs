use crate::collectionresolver::CollectionResolver;
use crate::error::{Error, Result};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{
    orchestrate_random_memd_client, KvClientManager, KvClientManagerClientType,
};
use crate::memdx::request::GetCollectionIdRequest;
use crate::memdx::response::GetCollectionIdResponse;
use futures::TryFutureExt;
use std::sync::Arc;

pub(crate) struct CollectionResolverMemd<K> {
    conn_mgr: Arc<K>,
}

pub(crate) struct CollectionResolverMemdOptions<K> {
    pub conn_mgr: Arc<K>,
}

impl<K> CollectionResolverMemd<K>
where
    K: KvClientManager,
{
    pub fn new(opts: CollectionResolverMemdOptions<K>) -> Self {
        Self {
            conn_mgr: opts.conn_mgr,
        }
    }
}

impl<K> CollectionResolver for CollectionResolverMemd<K>
where
    K: KvClientManager,
{
    async fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> Result<(u32, u64)> {
        let resp: GetCollectionIdResponse = orchestrate_random_memd_client(
            self.conn_mgr.clone(),
            async |client: Arc<KvClientManagerClientType<K>>| {
                client
                    .get_collection_id(GetCollectionIdRequest {
                        scope_name,
                        collection_name,
                    })
                    .map_err(Error::new_contextual_memdx_error)
                    .await
            },
        )
        .await?;

        Ok((resp.collection_id, resp.manifest_rev))
    }

    async fn invalidate_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
        endpoint: &str,
        manifest_rev: u64,
    ) {
    }
}
