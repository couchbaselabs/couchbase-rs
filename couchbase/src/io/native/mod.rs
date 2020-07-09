mod endpoint;
mod util;

use crate::io::request::Request;
use log::debug;

pub struct IoCore {}

impl IoCore {
    pub fn new(connection_string: String, username: String, password: String) -> Self {
        debug!("Using native IO transport");
        Self {}
    }

    pub fn send(&self, request: Request) {}

    pub fn open_bucket(&self, name: String) {}
}
