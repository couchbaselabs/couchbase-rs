//! Everything regarding Documents and their usage.
use std::str::{from_utf8, Utf8Error};
use std::fmt;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{from_slice, to_vec, to_string};

pub trait Document {
    type Content;

    fn id(&self) -> &str;
    fn cas(&self) -> u64;
    fn content(self) -> Self::Content;
    fn content_ref(&self) -> &Self::Content;
    fn content_mut(&mut self) -> &mut Self::Content;
    fn content_as_vec(self) -> Vec<u8>;
    fn expiry(&self) -> u32;
    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
        where S: Into<String>;
    fn flags(&self) -> u32;
}

pub struct BinaryDocument {
    id: String,
    cas: u64,
    content: Option<Vec<u8>>,
    expiry: u32,
}

impl BinaryDocument {
    pub fn content_as_str(&self) -> Result<&str, Utf8Error> {
        from_utf8(self.content_ref())
    }
}

impl Document for BinaryDocument {
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

    fn content_as_vec(self) -> Vec<u8> {
        self.content()
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
        Self {
            id: id.into(),
            cas: cas.unwrap_or(0),
            content: content,
            expiry: expiry.unwrap_or(0),
        }
    }

    fn flags(&self) -> u32 {
        3 << 24
    }
}

impl fmt::Debug for BinaryDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = self.content.as_ref().map(|c| from_utf8(c).unwrap_or("<not utf8 decodable>"));
        write!(f,
               "BinaryDocument {{ id: \"{}\", cas: {}, expiry: {}, content: {:?} }}",
               self.id,
               self.cas,
               self.expiry,
               content)
    }
}

pub struct JsonDocument<T> {
    id: String,
    cas: u64,
    content: Option<T>,
    expiry: u32,
}


impl<T> Document for JsonDocument<T> where T: Serialize + DeserializeOwned {
    type Content = T;

    fn id(&self) -> &str {
        self.id.as_ref()
    }

    fn cas(&self) -> u64 {
        self.cas
    }

    fn content(self) -> T {
        self.content.unwrap()
    }

    fn content_as_vec(self) -> Vec<u8> {
        to_vec(self.content_ref()).unwrap()
    }

    fn content_ref(&self) -> &T {
        self.content.as_ref().unwrap()
    }

    fn content_mut(&mut self) -> &mut T {
        self.content.as_mut().unwrap()
    }

    fn expiry(&self) -> u32 {
        self.expiry
    }

    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
        where S: Into<String>
    {
        Self {
            id: id.into(),
            cas: cas.unwrap_or(0),
            content: content.map(|v| from_slice(&v).unwrap()),
            expiry: expiry.unwrap_or(0),
        }
    }

    fn flags(&self) -> u32 {
        2 << 24
    }
}

impl<T> fmt::Debug for JsonDocument<T> where T: Serialize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = to_string(&self.content);
        write!(f,
               "JsonDocument {{ id: \"{}\", cas: {}, expiry: {}, content: {:?} }}",
               self.id,
               self.cas,
               self.expiry,
               content)
    }
}