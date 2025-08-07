use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub host: String,
    pub port: u16,
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
