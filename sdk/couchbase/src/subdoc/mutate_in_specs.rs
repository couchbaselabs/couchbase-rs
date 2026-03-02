/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

//! Specifications for sub-document mutation operations.
//!
//! Use [`MutateInSpec`] to build individual mutation specs, then pass a `Vec<MutateInSpec>` to
//! [`Collection::mutate_in`](crate::collection::Collection).
//!
//! # Example
//!
//! ```rust,no_run
//! use couchbase::subdoc::mutate_in_specs::MutateInSpec;
//!
//! # async fn example(collection: couchbase::collection::Collection) -> couchbase::error::Result<()> {
//! let specs = vec![
//!     MutateInSpec::upsert("name", "Alice", None)?,
//!     MutateInSpec::array_append("tags", &["rust", "sdk"], None)?,
//!     MutateInSpec::increment("login_count", 1, None)?,
//!     MutateInSpec::remove("temp_field", None),
//! ];
//!
//! let result = collection.mutate_in("doc-id", &specs, None).await?;
//! println!("New CAS: {}", result.cas());
//! # Ok(())
//! # }
//! ```

use crate::error;
use crate::error::Error;
use crate::subdoc::macros::MUTATE_IN_MACROS;
use couchbase_core::memdx::subdoc::MutateInOpType::{
    ArrayAddUnique, ArrayInsert, ArrayPushFirst, ArrayPushLast, Counter, Delete, DeleteDoc,
    DictAdd, DictSet, Replace, SetDoc,
};
use couchbase_core::memdx::subdoc::{MutateInOp, SubdocOp, SubdocOpFlag};
use serde::Serialize;

/// A sub-document mutation specification, used with
/// [`Collection::mutate_in`](crate::collection::Collection).
///
/// Create specs using the static constructors such as [`insert`](MutateInSpec::insert),
/// [`upsert`](MutateInSpec::upsert), [`replace`](MutateInSpec::replace),
/// [`remove`](MutateInSpec::remove), [`array_append`](MutateInSpec::array_append),
/// [`array_prepend`](MutateInSpec::array_prepend), [`array_insert`](MutateInSpec::array_insert),
/// [`array_add_unique`](MutateInSpec::array_add_unique),
/// [`increment`](MutateInSpec::increment), and [`decrement`](MutateInSpec::decrement).
///
/// # Example
///
/// ```rust
/// use couchbase::subdoc::mutate_in_specs::MutateInSpec;
///
/// let specs = vec![
///     MutateInSpec::upsert("name", "Alice", None).unwrap(),
///     MutateInSpec::remove("temp", None),
/// ];
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MutateInSpec {
    /// The type of mutation operation.
    pub op: MutateInOpType,
    /// The JSON path to mutate.
    pub path: String,
    /// The serialized value to write (empty for remove operations).
    pub value: Vec<u8>,
    /// Whether to create intermediate path elements if they don't exist.
    pub create_path: bool,
    /// Whether this operation targets an extended attribute (xattr).
    pub is_xattr: bool,
    /// Whether server-side macro expansion should be applied.
    pub expand_macros: bool,
}

impl SubdocOp for MutateInSpec {
    fn is_xattr_op(&self) -> bool {
        self.is_xattr
    }
}

/// The type of a sub-document mutation operation.
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum MutateInOpType {
    /// Insert a value at the path (fails if the path already exists).
    Insert,
    /// Set the value at the path (creates or overwrites).
    Upsert,
    /// Replace the value at the path (fails if the path does not exist).
    Replace,
    /// Remove the value at the path.
    Remove,
    /// Append value(s) to an array at the path.
    ArrayAppend,
    /// Prepend value(s) to an array at the path.
    ArrayPrepend,
    /// Insert a value at a specific array index.
    ArrayInsert,
    /// Add a value to an array only if it doesn't already exist.
    ArrayAddUnique,
    /// Increment or decrement a numeric value at the path.
    Counter,
}

/// Options for a [`MutateInSpec::insert`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct InsertSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl InsertSpecOptions {
    /// Creates a new `InsertSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::upsert`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct UpsertSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl UpsertSpecOptions {
    /// Creates a new `UpsertSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::replace`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ReplaceSpecOptions {
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl ReplaceSpecOptions {
    /// Creates a new `ReplaceSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

/// Options for a [`MutateInSpec::remove`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RemoveSpecOptions {
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl RemoveSpecOptions {
    /// Creates a new `RemoveSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

/// Options for a [`MutateInSpec::array_append`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayAppendSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl ArrayAppendSpecOptions {
    /// Creates a new `ArrayAppendSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::array_prepend`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayPrependSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl ArrayPrependSpecOptions {
    /// Creates a new `ArrayPrependSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::array_insert`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayInsertSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl ArrayInsertSpecOptions {
    /// Creates a new `ArrayInsertSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::array_add_unique`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ArrayAddUniqueSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl ArrayAddUniqueSpecOptions {
    /// Creates a new `ArrayAddUniqueSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::increment`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct IncrementSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl IncrementSpecOptions {
    /// Creates a new `IncrementSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

/// Options for a [`MutateInSpec::decrement`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DecrementSpecOptions {
    /// If `true`, create intermediate path elements if they don't exist.
    pub create_path: Option<bool>,
    /// If `true`, target an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl DecrementSpecOptions {
    /// Creates a new `DecrementSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }

    /// Sets whether to create intermediate path elements.
    pub fn create_path(mut self, create_path: bool) -> Self {
        self.create_path = Some(create_path);
        self
    }
}

impl MutateInSpec {
    /// Creates an `insert` mutation spec that inserts a value at the given path.
    ///
    /// Fails if the path already exists. The value is serialized to JSON via `serde`.
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

    /// Creates an `insert` mutation spec with pre-encoded raw bytes.
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

    /// Creates an `upsert` mutation spec that sets the value at the given path,
    /// creating or overwriting it.
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

    /// Creates an `upsert` mutation spec with pre-encoded raw bytes.
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

    /// Creates a `replace` mutation spec that replaces the value at the given path.
    ///
    /// Fails if the path does not exist.
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

    /// Creates a `replace` mutation spec with pre-encoded raw bytes.
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

    /// Creates a `remove` mutation spec that removes the value at the given path.
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

    /// Appends one or more values to the end of an array at the given path.
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

    /// Appends raw bytes to the end of an array at the given path.
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

    /// Prepends one or more values to the beginning of an array at the given path.
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

    /// Prepends raw bytes to the beginning of an array at the given path.
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

    /// Inserts one or more values at a specific position in an array.
    ///
    /// The path must include the array index, e.g. `"tags[2]"`.
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

    /// Inserts raw bytes at a specific position in an array.
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

    /// Adds a unique value to an array at the given path (no-op if it already exists).
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

    /// Adds raw bytes as a unique value to an array at the given path.
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

    /// Increments a numeric value at the given path by the specified positive delta.
    ///
    /// Returns an error if `delta` is negative.
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

    /// Decrements a numeric value at the given path by the specified positive delta.
    ///
    /// Returns an error if `delta` is negative.
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

impl<'a> TryFrom<&'a MutateInSpec> for MutateInOp<'a> {
    type Error = Error;

    fn try_from(value: &'a MutateInSpec) -> Result<Self, Self::Error> {
        let op_type = match value.op {
            MutateInOpType::Insert => {
                if value.path.is_empty() {
                    return Err(Error::invalid_argument(
                        "path",
                        "path cannot be empty for insert operation",
                    ));
                }

                DictAdd
            }
            MutateInOpType::Upsert => {
                if value.path.is_empty() {
                    return Err(Error::invalid_argument(
                        "path",
                        "path cannot be empty for upsert operation",
                    ));
                }

                DictSet
            }
            MutateInOpType::Replace => {
                if value.path.is_empty() {
                    SetDoc
                } else {
                    Replace
                }
            }
            MutateInOpType::Remove => {
                if value.path.is_empty() {
                    DeleteDoc
                } else {
                    Delete
                }
            }
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

        Ok(MutateInOp::new(op_type, value.path.as_bytes(), &value.value).flags(op_flags))
    }
}
