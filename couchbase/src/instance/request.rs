use crate::options::GetOptions;
use crate::result::{GetResult, MutationResult};
use couchbase_sys::*;
use futures::sync::oneshot::Sender;
use std::ffi::{c_void, CString};
use std::os::raw::c_char;
use std::ptr;

pub trait InstanceRequest: Send + 'static {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE);
}

#[derive(Debug)]
pub struct GetRequest {
    sender: Sender<Option<GetResult>>,
    id: String,
    options: Option<GetOptions>,
}

impl GetRequest {
    pub fn new(sender: Sender<Option<GetResult>>, id: String, options: Option<GetOptions>) -> Self {
        Self {
            sender,
            id,
            options,
        }
    }
}

impl InstanceRequest for GetRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");
        let mut command: *mut lcb_CMDGET = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;
        unsafe {
            lcb_cmdget_create(&mut command);
            lcb_cmdget_key(command, id_encoded.as_ptr(), id_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdget_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_get(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct UpsertRequest {
    sender: Sender<MutationResult>,
    id: String,
    content: Vec<u8>,
    flags: u32,
}

impl UpsertRequest {
    pub fn new(sender: Sender<MutationResult>, id: String, content: Vec<u8>, flags: u32) -> Self {
        Self {
            sender,
            id,
            content,
            flags,
        }
    }
}

impl InstanceRequest for UpsertRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");

        let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;

        let value_len = self.content.len();
        let value = CString::new(self.content).expect("Could not turn value into lcb format");

        unsafe {
            lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
            lcb_cmdstore_key(command, id_encoded.as_ptr(), id_len);
            lcb_cmdstore_flags(command, self.flags);
            lcb_cmdstore_value(command, value.into_raw() as *const c_char, value_len);
            lcb_store(instance, cookie, command);
        }
    }
}
