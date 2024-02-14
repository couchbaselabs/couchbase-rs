use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::magic::Magic;
use crate::memdx::op_bootstrap::OpBootstrapEncoder;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::RequestPacket;
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::request::HelloRequest;
use crate::memdx::response::HelloResponse;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::warn;
use std::io::Cursor;
use std::sync::mpsc::Sender;

pub struct OpsCore {}

impl OpBootstrapEncoder for OpsCore {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
        result_sender: Sender<crate::memdx::client::Result<HelloResponse>>,
    ) -> crate::memdx::client::Result<ClientPendingOp>
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
                        let mut features: Vec<HelloFeature> = Vec::new();
                        if let Some(value) = packet.value() {
                            let mut cursor = Cursor::new(value);
                            while let Ok(code) = cursor.read_u16::<BigEndian>() {
                                features.push(HelloFeature::from(code));
                            }
                        }
                        let response = HelloResponse {
                            enabled_features: features,
                        };

                        match result_sender.send(Ok(response)) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Failed to send result on result channel {}", e);
                            }
                        };
                    }
                    Err(e) => {
                        match result_sender.send(Err(e)) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Failed to send error on result channel {}", e);
                            }
                        };
                    }
                };

                false
            })
            .await
    }
}
