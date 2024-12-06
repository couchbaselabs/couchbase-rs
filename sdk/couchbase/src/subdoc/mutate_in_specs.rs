use crate::error;
use crate::subdoc::macros::MUTATE_IN_MACROS;
use crate::transcoding::json;
use bytes::{BufMut, Bytes, BytesMut};
use couchbase_core::memdx::subdoc::MutateInOpType::{
    ArrayAddUnique, ArrayInsert, ArrayPushFirst, ArrayPushLast, Counter, Delete, DictAdd, DictSet,
    Replace,
};
use couchbase_core::memdx::subdoc::{MutateInOp, SubdocOp, SubdocOpFlag};
use serde::Serialize;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[non_exhaustive]
pub struct MutateInSpec {
    pub op: MutateInOpType,
    pub path: String,
    pub value: Bytes,
    pub create_path: bool,
    pub is_xattr: bool,
    pub expand_macros: bool,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct InsertSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct UpsertSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ReplaceSpecOptions {
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct RemoveSpecOptions {
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ArrayAppendSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ArrayPrependSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ArrayInsertSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ArrayAddUniqueSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct IncrementSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct DecrementSpecOptions {
    pub create_path: Option<bool>,
    pub is_xattr: Option<bool>,
}

fn join_values(values: &[Bytes]) -> Bytes {
    if values.is_empty() {
        return Bytes::new();
    }

    if values.len() == 1 {
        return values[0].clone();
    }

    let mut total_length = values.len() - 1;
    for v in values {
        total_length += v.len();
    }

    let mut result = BytesMut::with_capacity(total_length);
    let mut first = true;

    for value in values {
        if !first {
            result.put(&[b','][..]);
        }
        result.extend_from_slice(value);
        first = false;
    }

    result.freeze()
}

impl MutateInSpec {
    pub fn insert<V: Serialize>(
        path: impl Into<String>,
        value: V,
        opts: impl Into<Option<InsertSpecOptions>>,
    ) -> error::Result<Self> {
        Ok(Self::insert_raw(path, json::encode(value)?.content, opts))
    }

    pub fn insert_raw(
        path: impl Into<String>,
        value: Bytes,
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
        Ok(Self::upsert_raw(path, json::encode(value)?.content, opts))
    }

    pub fn upsert_raw(
        path: impl Into<String>,
        value: Bytes,
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
        Ok(Self::replace_raw(path, json::encode(value)?.content, opts))
    }

    pub fn replace_raw(
        path: impl Into<String>,
        value: Bytes,
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
            value: Bytes::new(),
        }
    }

    pub fn array_append<V: Serialize>(
        path: impl Into<String>,
        value: &[V],
        opts: impl Into<Option<ArrayAppendSpecOptions>>,
    ) -> error::Result<Self> {
        let serialized_values = value
            .iter()
            .map(|value| json::encode(value).map(|encoded| encoded.content))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::array_append_raw(
            path,
            join_values(&serialized_values),
            opts,
        ))
    }

    pub fn array_append_raw(
        path: impl Into<String>,
        value: Bytes,
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
        let serialized_values = value
            .iter()
            .map(|value| json::encode(value).map(|encoded| encoded.content))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::array_prepend_raw(
            path,
            join_values(&serialized_values),
            opts,
        ))
    }

    pub fn array_prepend_raw(
        path: impl Into<String>,
        value: Bytes,
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
        let serialized_values = value
            .iter()
            .map(|value| json::encode(value).map(|encoded| encoded.content))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::array_insert_raw(
            path,
            join_values(&serialized_values),
            opts,
        ))
    }

    pub fn array_insert_raw(
        path: impl Into<String>,
        value: Bytes,
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
            json::encode(value)?.content,
            opts,
        ))
    }

    pub fn array_add_unique_raw(
        path: impl Into<String>,
        value: Bytes,
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
            return Err(error::Error {
                msg: "only positive delta allowed in subdoc increment".to_string(),
            });
        }

        let value = json::encode(delta)?.content;
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
            return Err(error::Error {
                msg: "only positive delta allowed in subdoc decrement".to_string(),
            });
        }

        let value = json::encode(-delta)?.content;
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

    fn check_is_macro(value: &Bytes) -> bool {
        MUTATE_IN_MACROS.contains(&value.as_ref())
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

        MutateInOp {
            op: op_type,
            flags: op_flags,
            path: value.path.as_bytes(),
            value: &value.value,
        }
    }
}
