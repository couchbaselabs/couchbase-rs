use std::future::Future;

use crate::memdx::client::MemdxResult;
use crate::memdx::pendingop::{PendingOp, StandardPendingOp};
use crate::memdx::response::TryFromClientResponse;

pub async fn sync_unary_call<RespT, Fut>(fut: Fut) -> MemdxResult<RespT>
where
    RespT: TryFromClientResponse,
    Fut: Future<Output = MemdxResult<StandardPendingOp<RespT>>>,
{
    let mut op = fut.await?;

    op.recv().await
}
