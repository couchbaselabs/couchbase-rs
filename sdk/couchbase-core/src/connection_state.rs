#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Connecting,
    Disconnecting,
}
