//! Everything regarding Documents and their usage.
use std::str::{Utf8Error, from_utf8};
use std::fmt;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{from_slice, to_string, to_vec};

pub trait Document {
    type Content;

    fn id(&self) -> &str;
    fn cas(&self) -> Option<u64>;
    fn content(self) -> Option<Self::Content>;
    fn content_ref(&self) -> Option<&Self::Content>;
    fn content_mut(&mut self) -> Option<&mut Self::Content>;
    fn content_into_vec(self) -> Option<Vec<u8>>;
    fn expiry(&self) -> Option<u32>;
    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
    where
        S: Into<String>;
    fn flags(&self) -> u32;
}

pub struct BinaryDocument {
    id: String,
    cas: Option<u64>,
    content: Option<Vec<u8>>,
    expiry: Option<u32>,
}

impl BinaryDocument {
    pub fn content_as_str(&self) -> Result<Option<&str>, Utf8Error> {
        match self.content_ref() {
            Some(val) => from_utf8(val).map(|v| Some(v)),
            None => Ok(None),
        }
    }
}

impl Document for BinaryDocument {
    type Content = Vec<u8>;

    fn id(&self) -> &str {
        self.id.as_ref()
    }

    fn cas(&self) -> Option<u64> {
        self.cas
    }

    fn content(self) -> Option<Self::Content> {
        self.content
    }

    fn content_ref(&self) -> Option<&Self::Content> {
        self.content.as_ref()
    }

    fn content_mut(&mut self) -> Option<&mut Self::Content> {
        self.content.as_mut()
    }

    fn content_into_vec(self) -> Option<Vec<u8>> {
        self.content()
    }

    fn expiry(&self) -> Option<u32> {
        self.expiry
    }

    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
    where
        S: Into<String>,
    {
        Self {
            id: id.into(),
            cas: cas,
            content: content,
            expiry: expiry,
        }
    }

    fn flags(&self) -> u32 {
        3 << 24
    }
}

impl fmt::Debug for BinaryDocument {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = self.content
            .as_ref()
            .map(|c| from_utf8(c).unwrap_or("<not utf8 decodable>"));
        write!(
            f,
            "BinaryDocument {{ id: \"{}\", cas: {}, expiry: {}, content: {:?} }}",
            self.id,
            self.cas.unwrap_or(0),
            self.expiry.unwrap_or(0),
            content
        )
    }
}

pub struct JsonDocument<T> {
    id: String,
    cas: Option<u64>,
    content: Option<T>,
    expiry: Option<u32>,
}


impl<T> Document for JsonDocument<T>
where
    T: Serialize + DeserializeOwned,
{
    type Content = T;

    fn id(&self) -> &str {
        self.id.as_ref()
    }

    fn cas(&self) -> Option<u64> {
        self.cas
    }

    fn content(self) -> Option<Self::Content> {
        self.content
    }

    fn content_ref(&self) -> Option<&Self::Content> {
        self.content.as_ref()
    }

    fn content_mut(&mut self) -> Option<&mut Self::Content> {
        self.content.as_mut()
    }

    fn content_into_vec(self) -> Option<Vec<u8>> {
        match self.content_ref() {
            Some(ref v) => to_vec(v).ok(),
            None => None,
        }
    }

    fn expiry(&self) -> Option<u32> {
        self.expiry
    }

    fn create<S>(id: S, cas: Option<u64>, content: Option<Vec<u8>>, expiry: Option<u32>) -> Self
    where
        S: Into<String>,
    {
        Self {
            id: id.into(),
            cas: cas,
            content: content.map(|v| from_slice(&v).unwrap()),
            expiry: expiry,
        }
    }

    fn flags(&self) -> u32 {
        2 << 24
    }
}

impl<T> fmt::Debug for JsonDocument<T>
where
    T: Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = to_string(&self.content);
        write!(
            f,
            "JsonDocument {{ id: \"{}\", cas: {}, expiry: {}, content: {:?} }}",
            self.id,
            self.cas.unwrap_or(0),
            self.expiry.unwrap_or(0),
            content
        )
    }
}
