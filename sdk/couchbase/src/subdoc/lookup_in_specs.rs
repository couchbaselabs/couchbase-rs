use crate::subdoc::lookup_in_specs::LookupInOpType::{Count, Exists, Get};
use couchbase_core::memdx::subdoc::{LookupInOp, SubdocOp, SubdocOpFlag};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LookupInSpec {
    pub(crate) op: LookupInOpType,
    pub(crate) path: String,
    pub(crate) is_xattr: bool,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct GetSpecOptions {
    pub(crate) is_xattr: Option<bool>,
}

impl GetSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct ExistsSpecOptions {
    pub(crate) is_xattr: Option<bool>,
}

impl ExistsSpecOptions {
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
pub struct CountSpecOptions {
    pub(crate) is_xattr: Option<bool>,
}

impl CountSpecOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn xattr(mut self, is_xattr: bool) -> Self {
        self.is_xattr = Some(is_xattr);
        self
    }
}

impl LookupInSpec {
    pub fn get(path: impl Into<String>, opts: impl Into<Option<GetSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Get,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or_default(),
        }
    }

    pub fn exists(path: impl Into<String>, opts: impl Into<Option<ExistsSpecOptions>>) -> Self {
        let opts = opts.into().unwrap_or_default();
        Self {
            op: Exists,
            path: path.into(),
            is_xattr: opts.is_xattr.unwrap_or_default(),
        }
    }

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
