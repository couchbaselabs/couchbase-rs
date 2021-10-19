use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};

#[derive(Debug)]
pub struct GenericManagementResult {
    status: u16,
    payload: Option<Vec<u8>>,
}

impl GenericManagementResult {
    pub fn new(status: u16, payload: Option<Vec<u8>>) -> Self {
        Self { status, payload }
    }

    pub fn payload(&self) -> Option<&Vec<u8>> {
        self.payload.as_ref()
    }

    pub fn payload_or_error(&self) -> CouchbaseResult<&Vec<u8>> {
        match &self.payload {
            Some(p) => Ok(p),
            None => Err(CouchbaseError::Generic {
                ctx: ErrorContext::from(("", "result missing payload")),
            }),
        }
    }

    pub fn http_status(&self) -> u16 {
        self.status
    }
}
