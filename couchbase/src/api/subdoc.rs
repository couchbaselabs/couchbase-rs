use crate::api::subdoc_options::*;
use crate::io::{
    LOOKUPIN_MACRO_CAS, LOOKUPIN_MACRO_EXPIRYTIME, LOOKUPIN_MACRO_FLAGS, MUTATION_MACRO_CAS,
    MUTATION_MACRO_SEQNO, MUTATION_MACRO_VALUE_CRC32C,
};
use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use serde::{Serialize, Serializer};
use serde_json::{to_vec, Value};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MutationMacro {
    CAS,
    SeqNo,
    CRC32c,
}

impl Serialize for MutationMacro {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };
        serializer.serialize_str(alias)
    }
}

impl Display for MutationMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };

        write!(f, "{}", alias)
    }
}

impl Debug for MutationMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug)]
pub enum MutateInSpec {
    Replace {
        path: String,
        value: Vec<u8>,
        xattr: bool,
    },
    Insert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    Upsert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayAddUnique {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    Remove {
        path: String,
        xattr: bool,
    },
    Counter {
        path: String,
        delta: i64,
        create_path: bool,
        xattr: bool,
    },
    ArrayAppend {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayPrepend {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayInsert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
}

impl MutateInSpec {
    pub fn replace<S: Into<String>, T>(
        path: S,
        content: T,
        opts: ReplaceSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Replace {
            path: path.into(),
            value,
            xattr: opts.xattr,
        })
    }

    pub fn insert<S: Into<String>, T>(
        path: S,
        content: T,
        opts: InsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Insert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn upsert<S: Into<String>, T>(
        path: S,
        content: T,
        opts: UpsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Upsert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_add_unique<S: Into<String>, T>(
        path: S,
        content: T,
        opts: ArrayAddUniqueSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::ArrayAddUnique {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_append<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayAppendSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayAppend {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_prepend<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayPrependSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayPrepend {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_insert<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayInsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayInsert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn remove<S: Into<String>>(path: S, opts: RemoveSpecOptions) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Remove {
            path: path.into(),
            xattr: opts.xattr,
        })
    }

    pub fn increment<S: Into<String>>(
        path: S,
        delta: u64,
        opts: IncrementSpecOptions,
    ) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Counter {
            path: path.into(),
            delta: delta as i64,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn decrement<S: Into<String>>(
        path: S,
        delta: u64,
        opts: DecrementSpecOptions,
    ) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Counter {
            path: path.into(),
            delta: -(delta as i64),
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum LookupinMacro {
    CAS,
    ExpiryTime,
    Flags,
}

impl Serialize for LookupinMacro {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let alias = match *self {
            LookupinMacro::CAS => LOOKUPIN_MACRO_CAS,
            LookupinMacro::ExpiryTime => LOOKUPIN_MACRO_EXPIRYTIME,
            LookupinMacro::Flags => LOOKUPIN_MACRO_FLAGS,
        };
        serializer.serialize_str(alias)
    }
}

impl Display for LookupinMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            LookupinMacro::CAS => LOOKUPIN_MACRO_CAS,
            LookupinMacro::ExpiryTime => LOOKUPIN_MACRO_EXPIRYTIME,
            LookupinMacro::Flags => LOOKUPIN_MACRO_FLAGS,
        };

        write!(f, "{}", alias)
    }
}

impl Debug for LookupinMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            LookupinMacro::CAS => LOOKUPIN_MACRO_CAS,
            LookupinMacro::ExpiryTime => LOOKUPIN_MACRO_EXPIRYTIME,
            LookupinMacro::Flags => LOOKUPIN_MACRO_FLAGS,
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug)]
pub enum LookupInSpec {
    Get { path: String, xattr: bool },
    Exists { path: String, xattr: bool },
    Count { path: String, xattr: bool },
}

impl LookupInSpec {
    pub fn get<S: Into<String>>(path: S, opts: GetSpecOptions) -> Self {
        LookupInSpec::Get {
            path: path.into(),
            xattr: opts.xattr,
        }
    }

    pub fn exists<S: Into<String>>(path: S, opts: ExistsSpecOptions) -> Self {
        LookupInSpec::Exists {
            path: path.into(),
            xattr: opts.xattr,
        }
    }

    pub fn count<S: Into<String>>(path: S, opts: CountSpecOptions) -> Self {
        LookupInSpec::Count {
            path: path.into(),
            xattr: opts.xattr,
        }
    }
}
