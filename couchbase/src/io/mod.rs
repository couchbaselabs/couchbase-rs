use crate::io::request::Request;

#[cfg(feature = "libcouchbase")]
mod lcb;

#[cfg(feature = "libcouchbase")]
use crate::io::lcb::IoCore;

pub mod request;
pub(crate) use lcb::couchbase_error_from_lcb_status;
pub(crate) use lcb::{
    LOOKUPIN_MACRO_CAS, LOOKUPIN_MACRO_EXPIRYTIME, LOOKUPIN_MACRO_FLAGS, MUTATION_MACRO_CAS,
    MUTATION_MACRO_SEQNO, MUTATION_MACRO_VALUE_CRC32C,
};

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
