use crate::subdoc::lookup_in_specs::LookupInOpType::{Count, Exists, Get};
use couchbase_core::memdx::subdoc::{LookupInOp, SubdocOp, SubdocOpFlag};
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[non_exhaustive]
pub struct LookupInSpec {
    pub op: LookupInOpType,
    pub path: String,
    pub is_xattr: bool,
}

impl SubdocOp for LookupInSpec {
    fn is_xattr_op(&self) -> bool {
        self.is_xattr
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum LookupInOpType {
    Get,
    Exists,
    Count,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct GetSpecOptions {
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ExistsSpecOptions {
    pub is_xattr: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct CountSpecOptions {
    pub is_xattr: Option<bool>,
}

impl LookupInSpec {
    pub fn get(path: impl Into<String>, opts: impl Into<Option<GetSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Get,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or(false),
        }
    }

    pub fn exists(path: impl Into<String>, opts: impl Into<Option<ExistsSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Exists,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or(false),
        }
    }

    pub fn count(path: impl Into<String>, opts: impl Into<Option<CountSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Count,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or(false),
        }
    }
}

impl<'a> From<&'a LookupInSpec> for LookupInOp<'a> {
    fn from(value: &'a LookupInSpec) -> Self {
        let mut op_type = match value.op {
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

        if value.is_xattr {
            op_flags |= SubdocOpFlag::XATTR_PATH;
        }

        LookupInOp {
            op: op_type,
            flags: op_flags,
            path: value.path.as_bytes(),
        }
    }
}
