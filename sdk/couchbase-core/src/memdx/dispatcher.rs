/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::memdx::client::ResponseContext;
use crate::memdx::connection::ConnectionType;
use crate::memdx::error::Result;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;
use crate::orphan_reporter::OrphanContext;

pub type UnsolicitedPacketHandler =
    Arc<dyn Fn(ResponsePacket) -> BoxFuture<'static, ()> + Send + Sync>;
pub type OrphanResponseHandler = Arc<dyn Fn(ResponsePacket, OrphanContext) + Send + Sync>;
pub type OnReadLoopCloseHandler = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

pub struct DispatcherOptions {
    pub unsolicited_packet_handler: UnsolicitedPacketHandler,
    pub orphan_handler: Option<OrphanResponseHandler>,
    pub on_read_close_handler: OnReadLoopCloseHandler,
    pub disable_decompression: bool,
    pub id: String,
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
    fn new(conn: ConnectionType, opts: DispatcherOptions) -> Self;
    async fn dispatch<'a>(
        &self,
        packet: RequestPacket<'a>,
        is_persistent: bool,
        response_context: Option<ResponseContext>,
    ) -> Result<ClientPendingOp>;
    async fn close(&self) -> Result<()>;
}
