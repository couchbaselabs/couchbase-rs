use crate::error;
use crate::error::Error;
use crate::subdoc::macros::MUTATE_IN_MACROS;
use couchbase_core::memdx::subdoc::MutateInOpType::{
    ArrayAddUnique, ArrayInsert, ArrayPushFirst, ArrayPushLast, Counter, Delete, DictAdd, DictSet,
    Replace,
};
use couchbase_core::memdx::subdoc::{MutateInOp, SubdocOp, SubdocOpFlag};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
// TODO: Should this be an enum?
pub struct MutateInSpec {
    pub(crate) op: MutateInOpType,
    pub(crate) path: String,
    pub(crate) value: Vec<u8>,
    pub(crate) create_path: bool,
    pub(crate) is_xattr: bool,
    pub(crate) expand_macros: bool,
}

impl SubdocOp for MutateInSpec {
    fn is_xattr_op(&self) -> bool {
        self.is_xattr
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutateInOpType {
    Insert,
    Upsert,
    Replace,
    Remove,
    ArrayAppend,
    ArrayPrepend,
    ArrayInsert,
    ArrayAddUnique,
    Counter,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct InsertSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl InsertSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct UpsertSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl UpsertSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ReplaceSpecOptions {
    pub(crate) is_xattr: Option<bool>,
}

impl ReplaceSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RemoveSpecOptions {
    pub(crate) is_xattr: Option<bool>,
}

impl RemoveSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayAppendSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl ArrayAppendSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayPrependSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl ArrayPrependSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayInsertSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl ArrayInsertSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayAddUniqueSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl ArrayAddUniqueSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct IncrementSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl IncrementSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DecrementSpecOptions {
    pub(crate) create_path: Option<bool>,
    pub(crate) is_xattr: Option<bool>,
}

impl DecrementSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

impl MutateInSpec {
    pub fn insert<V: Serialize>(
        path: impl Into<String>,
        value: V,
        opts: impl Into<Option<InsertSpecOptions>>,
    ) -> error::Result<Self> {
        Ok(Self::insert_raw(
            path,
            serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?,
            opts,
        ))
    }

    pub fn insert_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<InsertSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();
        let is_macro = Self::check_is_macro(&value);

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(is_macro),
            expand_macros: is_macro,
            op: MutateInOpType::Insert,
            path: path.into(),
            value,
        }
    }

    pub fn upsert<V: Serialize>(
        path: impl Into<String>,
        value: V,
        opts: impl Into<Option<UpsertSpecOptions>>,
    ) -> error::Result<Self> {
        Ok(Self::upsert_raw(
            path,
            serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?,
            opts,
        ))
    }

    pub fn upsert_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<UpsertSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();
        let is_macro = Self::check_is_macro(&value);

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(is_macro),
            expand_macros: is_macro,
            op: MutateInOpType::Upsert,
            path: path.into(),
            value,
        }
    }

    pub fn replace<V: Serialize>(
        path: impl Into<String>,
        value: V,
        opts: impl Into<Option<ReplaceSpecOptions>>,
    ) -> error::Result<Self> {
        Ok(Self::replace_raw(
            path,
            serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?,
            opts,
        ))
    }

    pub fn replace_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<ReplaceSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();
        let is_macro = Self::check_is_macro(&value);

        Self {
            create_path: false,
            is_xattr: opts.is_xattr.unwrap_or(is_macro),
            expand_macros: is_macro,
            op: MutateInOpType::Replace,
            path: path.into(),
            value,
        }
    }

    pub fn remove(path: impl Into<String>, opts: impl Into<Option<RemoveSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();

        Self {
            create_path: false,
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::Remove,
            path: path.into(),
            value: Vec::new(),
        }
    }

    pub fn array_append<V: Serialize>(
        path: impl Into<String>,
        value: &[V],
        opts: impl Into<Option<ArrayAppendSpecOptions>>,
    ) -> error::Result<Self> {
        let mut value = serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?;
        if !value.is_empty() {
            value.remove(0);
            if !value.is_empty() {
                value.remove(value.len() - 1);
            }
        }

        Ok(Self::array_append_raw(path, value, opts))
    }

    pub fn array_append_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<ArrayAppendSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::ArrayAppend,
            path: path.into(),
            value,
        }
    }

    pub fn array_prepend<V: Serialize>(
        path: impl Into<String>,
        value: &[V],
        opts: impl Into<Option<ArrayPrependSpecOptions>>,
    ) -> error::Result<Self> {
        let mut value = serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?;
        if !value.is_empty() {
            value.remove(0);
            if !value.is_empty() {
                value.remove(value.len() - 1);
            }
        }

        Ok(Self::array_prepend_raw(path, value, opts))
    }

    pub fn array_prepend_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<ArrayPrependSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::ArrayPrepend,
            path: path.into(),
            value,
        }
    }

    pub fn array_insert<V: Serialize>(
        path: impl Into<String>,
        value: &[V],
        opts: impl Into<Option<ArrayInsertSpecOptions>>,
    ) -> error::Result<Self> {
        let mut value = serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?;
        if !value.is_empty() {
            value.remove(0);
            if !value.is_empty() {
                value.remove(value.len() - 1);
            }
        }

        Ok(Self::array_insert_raw(path, value, opts))
    }

    pub fn array_insert_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<ArrayInsertSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::ArrayInsert,
            path: path.into(),
            value,
        }
    }

    pub fn array_add_unique<V: Serialize>(
        path: impl Into<String>,
        value: V,
        opts: impl Into<Option<ArrayAddUniqueSpecOptions>>,
    ) -> error::Result<Self> {
        Ok(Self::array_add_unique_raw(
            path,
            serde_json::to_vec(&value).map_err(Error::encoding_failure_from_serde)?,
            opts,
        ))
    }

    pub fn array_add_unique_raw(
        path: impl Into<String>,
        value: Vec<u8>,
        opts: impl Into<Option<ArrayAddUniqueSpecOptions>>,
    ) -> Self {
        let opts = opts.into().unwrap_or_default();
        let is_macro = Self::check_is_macro(&value);

        Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(is_macro),
            expand_macros: is_macro,
            op: MutateInOpType::ArrayAddUnique,
            path: path.into(),
            value,
        }
    }

    pub fn increment(
        path: impl Into<String>,
        delta: i64,
        opts: impl Into<Option<IncrementSpecOptions>>,
    ) -> error::Result<Self> {
        if delta < 0 {
            return Err(Error::invalid_argument(
                "delta",
                "only positive delta allowed in subdoc increment",
            ));
        }

        let value = serde_json::to_vec(&delta).map_err(Error::encoding_failure_from_serde)?;
        let opts = opts.into().unwrap_or_default();

        Ok(Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::Counter,
            path: path.into(),
            value,
        })
    }

    pub fn decrement(
        path: impl Into<String>,
        delta: i64,
        opts: impl Into<Option<DecrementSpecOptions>>,
    ) -> error::Result<Self> {
        if delta < 0 {
            return Err(Error::invalid_argument(
                "delta",
                "only positive delta allowed in subdoc increment",
            ));
        }

        let delta = -delta;
        let value = serde_json::to_vec(&delta).map_err(Error::encoding_failure_from_serde)?;
        let opts = opts.into().unwrap_or_default();

        Ok(Self {
            create_path: opts.create_path.unwrap_or(false),
            is_xattr: opts.is_xattr.unwrap_or(false),
            expand_macros: false,
            op: MutateInOpType::Counter,
            path: path.into(),
            value,
        })
    }

    fn check_is_macro(value: &[u8]) -> bool {
        MUTATE_IN_MACROS.contains(&value)
    }
}

impl<'a> From<&'a MutateInSpec> for MutateInOp<'a> {
    fn from(value: &'a MutateInSpec) -> Self {
        let op_type = match value.op {
            MutateInOpType::Insert => DictAdd,
            MutateInOpType::Upsert => DictSet,
            MutateInOpType::Replace => Replace,
            MutateInOpType::Remove => Delete,
            MutateInOpType::ArrayAppend => ArrayPushLast,
            MutateInOpType::ArrayPrepend => ArrayPushFirst,
            MutateInOpType::ArrayInsert => ArrayInsert,
            MutateInOpType::ArrayAddUnique => ArrayAddUnique,
            MutateInOpType::Counter => Counter,
        };

        let mut op_flags = SubdocOpFlag::empty();

        if value.is_xattr {
            op_flags |= SubdocOpFlag::XATTR_PATH;
        }
        if value.create_path {
            op_flags |= SubdocOpFlag::MKDIR_P;
        }
        if value.expand_macros {
            op_flags |= SubdocOpFlag::EXPAND_MACROS;
        }

        MutateInOp::new(op_type, value.path.as_bytes(), &value.value).flags(op_flags)
    }
}
