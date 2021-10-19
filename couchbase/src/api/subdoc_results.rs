use crate::io::couchbase_error_from_lcb_status;
use crate::{CouchbaseError, CouchbaseResult, ErrorContext};

#[derive(Debug)]
pub(crate) struct SubDocField {
    pub status: u32,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct MutateInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl MutateInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

#[derive(Debug)]
pub struct LookupInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl LookupInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub(crate) fn raw(&self, index: usize) -> CouchbaseResult<&Vec<u8>> {
        let content = match self.content.get(index) {
            Some(c) => c,
            None => {
                return Err(CouchbaseError::InvalidArgument {
                    ctx: ErrorContext::from((index.to_string().as_str(), "index not found")),
                })
            }
        };

        match content.status {
            0 => {}
            _ => {
                return Err(couchbase_error_from_lcb_status(
                    content.status,
                    ErrorContext::default(),
                ))
            }
        }

        Ok(&content.value)
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self, index: usize) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        let content = self.raw(index)?;

        serde_json::from_slice(content.as_slice())
            .map_err(CouchbaseError::decoding_failure_from_serde)
    }

    pub fn exists(&self, index: usize) -> bool {
        let content = match self.content.get(index) {
            Some(c) => c,
            None => return false,
        };
        content.status == 0
    }
}
