use std::future::Future;
use std::sync::Arc;

use crate::error::ErrorKind::CollectionManifestOutdated;
use crate::error::Result;
use crate::memdx::error::ServerError;

pub(crate) trait CollectionResolver: Sized + Send + Sync {
    fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> impl Future<Output = Result<(u32, u64)>> + Send;

    fn invalidate_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
        endpoint: &str,
        manifest_rev: u64,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) async fn orchestrate_memd_collection_id<Cr, Resp, Fut>(
    resolver: Arc<Cr>,
    scope_name: &str,
    collection_name: &str,
    mut operation: impl Fn(u32, u64) -> Fut,
) -> Result<Resp>
where
    Cr: CollectionResolver,
    Fut: Future<Output = Result<Resp>> + Send,
{
    let (mut collection_id, mut manifest_rev) = resolver
        .resolve_collection_id(scope_name, collection_name)
        .await?;

    loop {
        let err = match operation(collection_id, manifest_rev).await {
            Ok(r) => return Ok(r),
            Err(e) => e,
        };

        let invalidating_manifest_rev = if let Some(memdx_err) = err.is_memdx_error() {
            if memdx_err.is_dispatch_error() {
                if let Some(ctx) = memdx_err.has_server_error_context() {
                    if let Some(parsed) = ServerError::parse_context(ctx) {
                        parsed.manifest_rev
                    } else {
                        return Err(err);
                    }
                } else {
                    return Err(err);
                }
            } else {
                return Err(err);
            }
        } else {
            return Err(err);
        };

        let invalidating_manifest_rev = invalidating_manifest_rev.unwrap_or_default();
        if invalidating_manifest_rev > 0 && invalidating_manifest_rev < manifest_rev {
            return Err(CollectionManifestOutdated {
                manifest_uid: manifest_rev,
                server_manifest_uid: invalidating_manifest_rev,
            }
            .into());
        }

        resolver
            .invalidate_collection_id(scope_name, collection_name, "", invalidating_manifest_rev)
            .await;

        let (new_collection_id, new_manifest_rev) = resolver
            .resolve_collection_id(scope_name, collection_name)
            .await?;

        if new_collection_id == collection_id {
            return Err(CollectionManifestOutdated {
                manifest_uid: manifest_rev,
                server_manifest_uid: invalidating_manifest_rev,
            }
            .into());
        }

        collection_id = new_collection_id;
        manifest_rev = new_manifest_rev;
    }
}
