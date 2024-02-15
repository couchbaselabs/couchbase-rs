use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::magic::Magic;
use crate::memdx::op_bootstrap::OpBootstrapEncoder;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::request::{
    GetErrorMapRequest, HelloRequest, SASLAuthRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    GetErrorMapResponse, HelloResponse, SASLAuthResponse, SelectBucketResponse,
};
use crate::memdx::status::Status;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Write};

pub struct OpsCore {}

impl OpBootstrapEncoder for OpsCore {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
        cb: impl (Fn(Result<HelloResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher,
    {
        let mut features: Vec<u8> = Vec::new();
        for feature in request.requested_features {
            features.write_u16::<BigEndian>(feature.into()).unwrap();
        }
        let mut packet = RequestPacket::new(Magic::Req, OpCode::Hello);
        packet = packet.set_value(features);

        dispatcher
            .dispatch(packet, move |result| -> bool {
                match result {
                    Ok(packet) => {
                        let status = packet.status();
                        if status != Status::Success {
                            cb(Err(decode_error(packet)));
                            return false;
                        }

                        let mut features: Vec<HelloFeature> = Vec::new();
                        if let Some(value) = packet.value() {
                            if value.len() % 2 != 0 {
                                cb(Err(Error::Protocol("invalid hello features length".into())));
                                return false;
                            }

                            let mut cursor = Cursor::new(value);
                            while let Ok(code) = cursor.read_u16::<BigEndian>() {
                                features.push(HelloFeature::from(code));
                            }
                        }
                        let response = HelloResponse {
                            enabled_features: features,
                        };

                        cb(Ok(response));
                    }
                    Err(e) => {
                        cb(Err(e));
                    }
                };

                false
            })
            .await
    }

    async fn get_error_map<D>(
        &self,
        dispatcher: &mut D,
        request: GetErrorMapRequest,
        cb: impl (Fn(Result<GetErrorMapResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher,
    {
        let mut value = Vec::new();
        value.write_u16::<BigEndian>(request.version).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::GetErrorMap);
        packet = packet.set_value(value);

        dispatcher
            .dispatch(packet, move |result| -> bool {
                match result {
                    Ok(packet) => {
                        let status = packet.status();
                        if status != Status::Success {
                            cb(Err(decode_error(packet)));
                            return false;
                        }

                        // TODO: Clone?
                        let value = packet.value().clone().unwrap_or_default();
                        let response = GetErrorMapResponse { error_map: value };

                        cb(Ok(response));
                    }
                    Err(e) => {
                        cb(Err(e));
                    }
                };

                false
            })
            .await
    }

    async fn select_bucket<D>(
        &self,
        dispatcher: &mut D,
        request: SelectBucketRequest,
        cb: impl (Fn(Result<SelectBucketResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher,
    {
        let mut key = Vec::new();
        key.write_all(request.bucket_name.as_bytes()).unwrap();

        let mut packet = RequestPacket::new(Magic::Req, OpCode::SelectBucket);
        packet = packet.set_key(key);

        dispatcher
            .dispatch(packet, move |result| -> bool {
                match result {
                    Ok(packet) => {
                        let status = packet.status();
                        if status != Status::Success {
                            cb(Err(decode_error(packet)));
                            return false;
                        }

                        cb(Ok(SelectBucketResponse {}));
                    }
                    Err(e) => {
                        cb(Err(e));
                    }
                };

                false
            })
            .await
    }

    async fn sasl_auth<D>(
        &self,
        dispatcher: &mut D,
        request: SASLAuthRequest,
        cb: impl (Fn(Result<SASLAuthResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
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

        dispatcher
            .dispatch(packet, move |result| -> bool {
                match result {
                    Ok(packet) => {
                        let status = packet.status();
                        if status != Status::Success {
                            cb(Err(decode_error(packet)));
                            return false;
                        }

                        cb(Ok(SASLAuthResponse {
                            needs_more_steps: false,
                            // TODO: clone?
                            payload: packet.value().clone().unwrap_or_default(),
                        }));
                    }
                    Err(e) => {
                        cb(Err(e));
                    }
                };

                false
            })
            .await
    }
}

fn decode_error(resp: ResponsePacket) -> Error {
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
