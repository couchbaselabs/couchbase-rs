//! Everything regarding Documents and their usage.
use std::str::{from_utf8, Utf8Error};
use std::fmt;

pub trait Document {
    type Content: Into<Vec<u8>>;

    fn id(&self) -> &str;
    fn cas(&self) -> u64;
    fn content(self) -> Self::Content;
    fn content_ref(&self) -> &Self::Content;
    fn content_mut(&mut self) -> &mut Self::Content;
    fn expiry(&self) -> u32;
    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
        where S: Into<String>;
}

pub struct BytesDocument {
    id: String,
    cas: u64,
    content: Option<Vec<u8>>,
    expiry: u32,
}

impl BytesDocument {
    pub fn content_as_str(&self) -> Result<&str, Utf8Error> {
        from_utf8(self.content_ref())
    }
}

impl Document for BytesDocument {
    type Content = Vec<u8>;

    fn id(&self) -> &str {
        self.id.as_ref()
    }

    fn cas(&self) -> u64 {
        self.cas
    }

    fn content(self) -> Vec<u8> {
        self.content.unwrap()
    }

    fn content_ref(&self) -> &Vec<u8> {
        self.content.as_ref().unwrap()
    }

    fn content_mut(&mut self) -> &mut Vec<u8> {
        self.content.as_mut().unwrap()
    }

    fn expiry(&self) -> u32 {
        self.expiry
    }

    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
        where S: Into<String>
    {
        BytesDocument {
            id: id.into(),
            cas: cas.unwrap_or(0),
            content: content,
            expiry: expiry.unwrap_or(0),
        }
    }
}

impl fmt::Debug for BytesDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = self.content.as_ref().map(|c| from_utf8(c).unwrap_or("<not utf8 decodable>"));
        write!(f,
               "BytesDocument {{ id: \"{}\", cas: {}, expiry: {}, content: {:?} }}",
               self.id,
               self.cas,
               self.expiry,
               content)
    }
}
