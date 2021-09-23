use crate::io::request::Request;

#[cfg(feature = "libcouchbase")]
mod lcb;

#[cfg(feature = "libcouchbase")]
use crate::io::lcb::IoCore;

pub mod request;

#[derive(Debug)]
pub struct Core {
    io_core: IoCore,
}

impl Core {
    pub fn new(
        connection_string: String,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            io_core: IoCore::new(connection_string, username, password),
        }
    }

    pub fn send(&self, request: Request) {
        self.io_core.send(request)
    }

    pub fn open_bucket(&self, name: String) {
        self.io_core.open_bucket(name)
    }
}
