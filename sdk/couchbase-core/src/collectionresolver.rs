use crate::error::{Error, Result};
use crate::memdx::error::{ServerError, ServerErrorKind};
use std::future::Future;
use std::sync::Arc;

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
    mut operation: impl Fn(u32) -> Fut,
) -> Result<Resp>
where
    Cr: CollectionResolver,
    Fut: Future<Output = Result<Resp>> + Send,
{
    if (scope_name == "_default" && collection_name == "_default")
        || (scope_name.is_empty() && collection_name.is_empty())
    {
        // We never want to invalidate this id.
        return operation(0).await;
    }

    let (collection_id, manifest_rev) = match resolver
        .resolve_collection_id(scope_name, collection_name)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return Err(maybe_invalidate_collection_id(
                resolver,
                e,
                0,
                scope_name,
                collection_name,
            )
            .await);
        }
    };

    let err = match operation(collection_id).await {
        Ok(r) => return Ok(r),
        Err(e) => e,
    };

    Err(
        maybe_invalidate_collection_id(resolver, err, manifest_rev, scope_name, collection_name)
            .await,
    )
}

async fn maybe_invalidate_collection_id<Cr>(
    resolver: Arc<Cr>,
    err: Error,
    our_manifest_rev: u64,
    scope_name: &str,
    collection_name: &str,
) -> Error
where
    Cr: CollectionResolver,
{
    let invalidating_manifest_rev = match parse_manifest_rev_from_error(&err) {
        Some(rev) => rev,
        None => {
            return err;
        }
    };

    if invalidating_manifest_rev > 0 && invalidating_manifest_rev < our_manifest_rev {
        return err;
    }

    resolver
        .invalidate_collection_id(scope_name, collection_name, "", invalidating_manifest_rev)
        .await;

    err
}

fn parse_manifest_rev_from_error(e: &Error) -> Option<u64> {
    if let Some(memdx_err) = e.is_memdx_error() {
        if memdx_err.is_server_error_kind(ServerErrorKind::UnknownCollectionID)
            || memdx_err.is_server_error_kind(ServerErrorKind::UnknownCollectionName)
        {
            if let Some(ctx) = memdx_err.has_server_error_context() {
                if let Some(parsed) = ServerError::parse_context(ctx) {
                    return parsed.manifest_rev;
                }
            }
        }
    }

    None
}
