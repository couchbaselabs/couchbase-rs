use std::future::Future;

use crate::memdx::error::Result;
use crate::memdx::pendingop::{PendingOp, StandardPendingOp};
use crate::memdx::response::TryFromClientResponse;

pub async fn sync_unary_call<RespT, Fut>(fut: Fut) -> Result<RespT>
where
    RespT: TryFromClientResponse,
    Fut: Future<Output = Result<StandardPendingOp<RespT>>>,
{
    let mut op = fut.await?;

    op.recv().await
}
