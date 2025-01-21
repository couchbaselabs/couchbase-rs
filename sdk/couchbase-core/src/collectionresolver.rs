use crate::error::{Error, Result};
use crate::memdx::error::ServerError;
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
    mut operation: impl Fn(u32, u64) -> Fut,
) -> Result<Resp>
where
    Cr: CollectionResolver,
    Fut: Future<Output = Result<Resp>> + Send,
{
    let (collection_id, mut manifest_rev) = match resolver
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

    let err = match operation(collection_id, manifest_rev).await {
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
    let invalidating_manifest_rev = if let Some(manifest_rev) = parse_manifest_rev_from_error(&err)
    {
        manifest_rev
    } else {
        return err;
    };

    if our_manifest_rev > 0
        && invalidating_manifest_rev > 0
        && invalidating_manifest_rev < our_manifest_rev
    {
        return err;
    }

    resolver
        .invalidate_collection_id(scope_name, collection_name, "", invalidating_manifest_rev)
        .await;

    err
}

fn parse_manifest_rev_from_error(e: &Error) -> Option<u64> {
    if let Some(memdx_err) = e.is_memdx_error() {
        if memdx_err.is_unknown_collection_id_error()
            || memdx_err.is_unknown_collection_name_error()
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
