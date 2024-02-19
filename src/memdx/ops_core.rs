use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::magic::Magic;
use crate::memdx::op_bootstrap::{OpAuthEncoder, OpBootstrapEncoder};
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    GetErrorMapRequest, HelloRequest, SASLAuthRequest, SASLListMechsRequest, SASLStepRequest,
    SelectBucketRequest,
};
use crate::memdx::status::Status;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;
use tokio_util::sync::CancellationToken;

pub struct OpsCore {}

impl OpBootstrapEncoder for OpsCore {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        request: HelloRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        let mut features: Vec<u8> = Vec::new();
        for feature in request.requested_features {
            features.write_u16::<BigEndian>(feature.into()).unwrap();
        }
        let mut packet = RequestPacket::new(Magic::Req, OpCode::Hello);
        packet = packet.set_value(features);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }

    async fn get_error_map<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        request: GetErrorMapRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_u16::<BigEndian>(request.version).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::GetErrorMap);
        packet = packet.set_value(value);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }

    async fn select_bucket<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        request: SelectBucketRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        let mut key = Vec::new();
        key.write_all(request.bucket_name.as_bytes()).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::SelectBucket);
        packet = packet.set_key(key);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }

    async fn sasl_list_mechs<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        _request: SASLListMechsRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        let packet = RequestPacket::new(Magic::Req, OpCode::SASLListMechs);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }
}

impl OpAuthEncoder for OpsCore {
    async fn sasl_auth<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        request: SASLAuthRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        // TODO: Support more than PLAIN
        if request.auth_mechanism != AuthMechanism::Plain {
            return Err(Error::Unknown("not implemented".into()));
        }
        let mut value = Vec::new();
        value.write_all(request.payload.as_slice()).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::SASLAuth);
        packet = packet.set_key(request.auth_mechanism.into());
        packet = packet.set_value(value);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }

    async fn sasl_step<D>(
        &self,
        dispatcher: &mut D,
        cancellation_token: CancellationToken,
        request: SASLStepRequest,
    ) -> Result<StandardPendingOp>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_all(request.payload.as_slice()).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::SASLStep);
        packet = packet.set_key(request.auth_mechanism.into());
        packet = packet.set_value(value);

        let op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(op, cancellation_token))
    }
}

pub(crate) fn decode_error(resp: &ResponsePacket) -> Error {
    let status = resp.status();
    if status == Status::NotMyVbucket {
        Error::NotMyVbucket
    } else if status == Status::TmpFail {
        Error::TmpFail
    } else {
        Error::Unknown(format!("{}", status))
    }

    // TODO: decode error context
}
