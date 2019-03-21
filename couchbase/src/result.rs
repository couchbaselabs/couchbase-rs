use std::fmt;
use std::str;

use serde::Deserialize;
use serde_json::from_slice;

pub struct GetResult {
    cas: u64,
    encoded: Vec<u8>,
    flags: u32,
}

impl GetResult {
    pub fn new(cas: u64, encoded: Vec<u8>, flags: u32) -> Self {
        GetResult {
            cas,
            encoded,
            flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content_as<'a, T>(&'a self) -> T
    where
        T: Deserialize<'a>,
    {
        from_slice(&self.encoded.as_slice()).expect("Could not convert type")
    }
}

impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GetResult {{ cas: 0x{:x}, flags: 0x{:x}, encoded: {} }}",
            self.cas,
            self.flags,
            str::from_utf8(&self.encoded).unwrap()
        )
    }
}

pub struct MutationResult {
    cas: u64,
}

impl MutationResult {
    pub fn new(cas: u64) -> Self {
        MutationResult { cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl fmt::Debug for MutationResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MutationResult {{ cas: 0x{:x} }}", self.cas)
    }
}
