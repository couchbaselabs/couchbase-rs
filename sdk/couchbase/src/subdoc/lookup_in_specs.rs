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

//! Specifications for sub-document lookup operations.
//!
//! Use [`LookupInSpec`] to build individual lookup specs, then pass a `Vec<LookupInSpec>` to
//! [`Collection::lookup_in`](crate::collection::Collection).
//!
//! # Example
//!
//! ```rust,no_run
//! use couchbase::subdoc::lookup_in_specs::LookupInSpec;
//!
//! # async fn example(collection: couchbase::collection::Collection) -> couchbase::error::Result<()> {
//! let specs = vec![
//!     LookupInSpec::get("name", None),
//!     LookupInSpec::exists("email", None),
//!     LookupInSpec::count("tags", None),
//! ];
//!
//! let result = collection.lookup_in("doc-id", &specs, None).await?;
//! let name: String = result.content_as(0)?;
//! let email_exists: bool = result.exists(1)?;
//! let tag_count: u32 = result.content_as(2)?;
//! # Ok(())
//! # }
//! ```

use crate::subdoc::lookup_in_specs::LookupInOpType::{Count, Exists, Get};
use couchbase_core::memdx::subdoc::{LookupInOp, SubdocOp, SubdocOpFlag};

/// A sub-document lookup specification, used with
/// [`Collection::lookup_in`](crate::collection::Collection).
///
/// Create specs using the static constructors [`get`](LookupInSpec::get),
/// [`exists`](LookupInSpec::exists), and [`count`](LookupInSpec::count).
///
/// # Example
///
/// ```rust
/// use couchbase::subdoc::lookup_in_specs::LookupInSpec;
///
/// let specs = vec![
///     LookupInSpec::get("name", None),
///     LookupInSpec::exists("email", None),
///     LookupInSpec::count("tags", None),
/// ];
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LookupInSpec {
    /// The type of lookup operation.
    pub op: LookupInOpType,
    /// The JSON path to look up. Use `""` (empty string) to get the full document.
    pub path: String,
    /// Whether this operation targets an extended attribute (xattr).
    pub is_xattr: bool,
}

impl SubdocOp for LookupInSpec {
    fn is_xattr_op(&self) -> bool {
        self.is_xattr
    }
}

/// The type of a sub-document lookup operation.
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum LookupInOpType {
    /// Retrieve the value at the given path.
    Get,
    /// Check whether the given path exists.
    Exists,
    /// Count the number of elements in an array or object at the given path.
    Count,
}

/// Options for a [`LookupInSpec::get`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct GetSpecOptions {
    /// If `true`, read from an extended attribute (xattr) instead of the document body.
    pub is_xattr: Option<bool>,
}

impl GetSpecOptions {
    /// Creates a new `GetSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

/// Options for a [`LookupInSpec::exists`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ExistsSpecOptions {
    /// If `true`, check existence of an extended attribute (xattr) path.
    pub is_xattr: Option<bool>,
}

impl ExistsSpecOptions {
    /// Creates a new `ExistsSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

/// Options for a [`LookupInSpec::count`] operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CountSpecOptions {
    /// If `true`, count elements in an extended attribute (xattr) path.
    pub is_xattr: Option<bool>,
}

impl CountSpecOptions {
    /// Creates a new `CountSpecOptions` with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets whether this operation targets an extended attribute (xattr).
    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

impl LookupInSpec {
    /// Creates a `get` lookup spec to retrieve the value at the given path.
    ///
    /// Use `""` (empty string) as the path to retrieve the entire document.
    pub fn get(path: impl Into<String>, opts: impl Into<Option<GetSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Get,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or_default(),
        }
    }

    /// Creates an `exists` lookup spec to check whether the given path exists.
    pub fn exists(path: impl Into<String>, opts: impl Into<Option<ExistsSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Exists,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or_default(),
        }
    }

    /// Creates a `count` lookup spec to count elements in an array or object at the given path.
    pub fn count(path: impl Into<String>, opts: impl Into<Option<CountSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Count,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or_default(),
        }
    }
}

impl<'a> From<&'a LookupInSpec> for LookupInOp<'a> {
    fn from(value: &'a LookupInSpec) -> Self {
        let op_type = match value.op {
            Get => {
                if value.path.is_empty() {
                    couchbase_core::memdx::subdoc::LookupInOpType::GetDoc
                } else {
                    couchbase_core::memdx::subdoc::LookupInOpType::Get
                }
            }
            Exists => couchbase_core::memdx::subdoc::LookupInOpType::Exists,
            Count => couchbase_core::memdx::subdoc::LookupInOpType::GetCount,
        };

        let mut op_flags = SubdocOpFlag::empty();

        if value.is_xattr_op() {
            op_flags |= SubdocOpFlag::XATTR_PATH;
        }

        LookupInOp::new(op_type, value.path.as_bytes()).flags(op_flags)
    }
}
