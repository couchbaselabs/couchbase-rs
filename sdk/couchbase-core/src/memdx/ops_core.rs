use std::io::Write;

use byteorder::{BigEndian, WriteBytesExt};

use crate::memdx::client::MemdxResult;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::MemdxError;
use crate::memdx::magic::Magic;
use crate::memdx::op_auth_saslauto::OpSASLAutoEncoder;
use crate::memdx::op_auth_saslbyname::OpSASLAuthByNameEncoder;
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::op_auth_saslscram::OpSASLScramEncoder;
use crate::memdx::op_bootstrap::OpBootstrapEncoder;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    GetClusterConfigRequest, GetErrorMapRequest, HelloRequest, SASLAuthRequest,
    SASLListMechsRequest, SASLStepRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    GetClusterConfigResponse, GetErrorMapResponse, HelloResponse, SASLAuthResponse,
    SASLListMechsResponse, SASLStepResponse, SelectBucketResponse,
};
use crate::memdx::status::Status;

pub struct OpsCore {}

impl OpsCore {
    pub(crate) fn decode_error(resp: &ResponsePacket) -> MemdxError {
        let status = resp.status;
        if status == Status::NotMyVbucket {
            MemdxError::NotMyVbucket
        } else if status == Status::TmpFail {
            MemdxError::TmpFail
        } else {
            MemdxError::Unknown(format!("{}", status))
        }

        // TODO: decode error context
    }
}

impl OpBootstrapEncoder for OpsCore {
    async fn hello<D>(
        &self,
        dispatcher: &D,
        request: HelloRequest,
    ) -> MemdxResult<StandardPendingOp<HelloResponse>>
    where
        D: Dispatcher,
    {
        let mut features: Vec<u8> = Vec::new();
        for feature in request.requested_features {
            features.write_u16::<BigEndian>(feature.into()).unwrap();
        }

        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::Hello,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: None,
                value: Some(features),
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    async fn get_error_map<D>(
        &self,
        dispatcher: &D,
        request: GetErrorMapRequest,
    ) -> MemdxResult<StandardPendingOp<GetErrorMapResponse>>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_u16::<BigEndian>(request.version).unwrap();

        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::GetErrorMap,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: None,
                value: Some(value),
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    async fn select_bucket<D>(
        &self,
        dispatcher: &D,
        request: SelectBucketRequest,
    ) -> MemdxResult<StandardPendingOp<SelectBucketResponse>>
    where
        D: Dispatcher,
    {
        let mut key = Vec::new();
        key.write_all(request.bucket_name.as_bytes()).unwrap();

        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::SelectBucket,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: Some(key),
                value: None,
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }

    async fn get_cluster_config<D>(
        &self,
        dispatcher: &D,
        _request: GetClusterConfigRequest,
    ) -> MemdxResult<StandardPendingOp<GetClusterConfigResponse>>
    where
        D: Dispatcher,
    {
        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::GetClusterConfig,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: None,
                value: None,
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}

impl OpSASLPlainEncoder for OpsCore {
    async fn sasl_auth<D>(
        &self,
        dispatcher: &D,
        request: SASLAuthRequest,
    ) -> MemdxResult<StandardPendingOp<SASLAuthResponse>>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_all(request.payload.as_slice()).unwrap();

        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::SASLAuth,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: Some(request.auth_mechanism.into()),
                value: Some(value),
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}

impl OpSASLAuthByNameEncoder for OpsCore {}

impl OpSASLAutoEncoder for OpsCore {
    async fn sasl_list_mechs<D>(
        &self,
        dispatcher: &D,
        _request: SASLListMechsRequest,
    ) -> MemdxResult<StandardPendingOp<SASLListMechsResponse>>
    where
        D: Dispatcher,
    {
        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::SASLListMechs,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: None,
                value: None,
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}

impl OpSASLScramEncoder for OpsCore {
    async fn sasl_step<D>(
        &self,
        dispatcher: &D,
        request: SASLStepRequest,
    ) -> MemdxResult<StandardPendingOp<SASLStepResponse>>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_all(request.payload.as_slice()).unwrap();

        let op = dispatcher
            .dispatch(RequestPacket {
                magic: Magic::Req,
                op_code: OpCode::SASLStep,
                datatype: 0,
                vbucket_id: None,
                cas: None,
                extras: None,
                key: Some(request.auth_mechanism.into()),
                value: Some(value),
                framing_extras: None,
                opaque: None,
            })
            .await?;

        Ok(StandardPendingOp::new(op))
    }
}
