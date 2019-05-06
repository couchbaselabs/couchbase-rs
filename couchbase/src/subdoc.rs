use std::ffi::CString;

#[derive(Debug)]
pub struct LookupInSpec {
    path: CString,
    path_len: usize,
    command_type: SubdocLookupCommandType,
    xattr: bool,
}

impl LookupInSpec {
    pub fn get<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        let path = path.into();
        LookupInSpec {
            path_len: path.len(),
            path: CString::new(path).expect("Could not encode path"),
            command_type: SubdocLookupCommandType::Get,
            xattr: false,
        }
    }

    pub fn get_full_document() -> Self {
        LookupInSpec {
            path_len: 0,
            path: CString::new("").expect("Could not encode path"),
            command_type: SubdocLookupCommandType::GetDoc,
            xattr: false,
        }
    }

    pub fn count<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        let path = path.into();
        LookupInSpec {
            path_len: path.len(),

            path: CString::new(path).expect("Could not encode path"),
            command_type: SubdocLookupCommandType::Count,
            xattr: false,
        }
    }

    pub fn exists<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        let path = path.into();

        LookupInSpec {
            path_len: path.len(),

            path: CString::new(path).expect("Could not encode path"),
            command_type: SubdocLookupCommandType::Exists,
            xattr: false,
        }
    }

    pub fn xattr(mut self) -> Self {
        self.xattr = true;
        self
    }

    pub(crate) fn command_type(&self) -> &SubdocLookupCommandType {
        &self.command_type
    }

    pub(crate) fn path(&self) -> &CString {
        &self.path
    }

    pub(crate) fn path_len(&self) -> usize {
        self.path_len
    }
}

#[derive(Debug)]
pub enum SubdocLookupCommandType {
    Get,
    Exists,
    Count,
    GetDoc,
}
