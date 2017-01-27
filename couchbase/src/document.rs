//! Everything regarding Documents and their usage.
use std::str::{from_utf8, Utf8Error};
use std::fmt;
use std::io::Write;

pub struct Document {
    id: String,
    cas: u64,
    content: Vec<u8>,
    expiry: u32,
}

impl Document {
    pub fn from_str<'a, S>(id: S, content: &'a str) -> Self
        where S: Into<String>
    {
        Self::from_str_with_cas_and_expiry(id, content, 0, 0)
    }

    pub fn from_str_with_expiry<'a, S>(id: S, content: &'a str, expiry: u32) -> Self
        where S: Into<String>
    {
        Self::from_str_with_cas_and_expiry(id, content, 0, expiry)
    }

    pub fn from_str_with_cas<'a, S>(id: S, content: &'a str, cas: u64) -> Self
        where S: Into<String>
    {
        Self::from_str_with_cas_and_expiry(id, content, cas, 0)
    }

    pub fn from_str_with_cas_and_expiry<'a, S>(id: S,
                                               content: &'a str,
                                               cas: u64,
                                               expiry: u32)
                                               -> Self
        where S: Into<String>
    {
        let mut vc = Vec::with_capacity(content.len());
        vc.write_all(content.as_bytes()).expect("Could not convert content into vec");
        Self::from_vec_with_cas_and_expiry(id, vc, cas, expiry)
    }

    pub fn from_vec<S>(id: S, content: Vec<u8>) -> Self
        where S: Into<String>
    {
        Self::from_vec_with_cas_and_expiry(id, content, 0, 0)
    }

    pub fn from_vec_with_cas<S>(id: S, content: Vec<u8>, cas: u64) -> Self
        where S: Into<String>
    {
        Self::from_vec_with_cas_and_expiry(id, content, cas, 0)
    }

    pub fn from_vec_with_expiry<S>(id: S, content: Vec<u8>, expiry: u32) -> Self
        where S: Into<String>
    {
        Self::from_vec_with_cas_and_expiry(id, content, 0, expiry)
    }

    pub fn from_vec_with_cas_and_expiry<S>(id: S, content: Vec<u8>, cas: u64, expiry: u32) -> Self
        where S: Into<String>
    {
        Document {
            id: id.into(),
            cas: cas,
            content: content,
            expiry: expiry,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    pub fn content_as_ref(&self) -> &[u8] {
        self.content.as_ref()
    }

    pub fn content(self) -> Vec<u8> {
        self.content
    }

    pub fn content_as_str(&self) -> Result<&str, Utf8Error> {
        from_utf8(self.content_as_ref())
    }

    pub fn expiry(&self) -> u32 {
        self.expiry
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = self.content_as_str().unwrap_or("<not utf8 decodable>");
        write!(f,
               "Document {{ id: \"{}\", cas: {}, expiry: {}, content: {} }}",
               self.id,
               self.cas,
               self.expiry,
               content)
    }
}
