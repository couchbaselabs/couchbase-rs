use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::RequestPacket;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::GetCollectionIdRequest;
use crate::memdx::response::GetCollectionIdResponse;

pub struct OpsUtil {}

impl OpsUtil {
    pub async fn get_collection_id<'a, D>(
        &self,
        dispatcher: &D,
        request: GetCollectionIdRequest<'a>,
    ) -> Result<StandardPendingOp<GetCollectionIdResponse>>
    where
        D: Dispatcher,
    {
        let full_name = format!("{}.{}", request.scope_name, request.collection_name);

        let op = dispatcher
            .dispatch(
                RequestPacket {
                    magic: Magic::Req,
                    op_code: OpCode::GetCollectionId,
                    datatype: 0,
                    vbucket_id: None,
                    cas: None,
                    extras: None,
                    key: None,
                    value: Some(full_name.as_bytes()),
                    framing_extras: None,
                    opaque: None,
                },
                None,
            )
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}
