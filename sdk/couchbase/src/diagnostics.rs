use serde::Serialize;
use std::fmt::Display;

#[derive(Copy, Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Connecting,
    Disconnecting,
}

impl Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Connecting => "connecting",
            ConnectionState::Disconnecting => "disconnecting",
        };
        write!(f, "{txt}")
    }
}

impl From<couchbase_core::connection_state::ConnectionState> for ConnectionState {
    fn from(state: couchbase_core::connection_state::ConnectionState) -> Self {
        match state {
            couchbase_core::connection_state::ConnectionState::Connected => {
                ConnectionState::Connected
            }
            couchbase_core::connection_state::ConnectionState::Disconnected => {
                ConnectionState::Disconnected
            }
            couchbase_core::connection_state::ConnectionState::Connecting => {
                ConnectionState::Connecting
            }
            couchbase_core::connection_state::ConnectionState::Disconnecting => {
                ConnectionState::Disconnecting
            }
        }
    }
}
