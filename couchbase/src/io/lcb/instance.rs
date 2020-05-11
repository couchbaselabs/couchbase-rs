use crate::io::lcb::callbacks::*;
use crate::io::lcb::encode::into_cstring;
use crate::io::lcb::{encode_request, IoRequest};
use crate::io::request::Request;
use couchbase_sys::*;
use log::debug;
use std::collections::HashMap;
use std::os::raw::c_void;
use std::ptr;

/// Wraps a single `lcb_instance`.
pub struct LcbInstance {
    // The pointer to the actual libcouchbase instance
    inner: *mut lcb_INSTANCE,
}

impl LcbInstance {
    pub fn new<S: Into<Vec<u8>>>(
        connection_string: S,
        username: S,
        password: S,
    ) -> Result<Self, lcb_STATUS> {
        let mut inner: *mut lcb_INSTANCE = ptr::null_mut();
        let mut create_options: *mut lcb_CREATEOPTS = ptr::null_mut();
        let mut logger: *mut lcb_LOGGER = ptr::null_mut();
        let instance_cookie = Box::new(InstanceCookie::new());

        let (connection_string_len, connection_string) = into_cstring(connection_string);
        let (username_len, username) = into_cstring(username);
        let (password_len, password) = into_cstring(password);

        unsafe {
            check_lcb_status(lcb_createopts_create(
                &mut create_options,
                lcb_INSTANCE_TYPE_LCB_TYPE_CLUSTER,
            ))?;
            check_lcb_status(lcb_logger_create(&mut logger, ptr::null_mut()))?;
            check_lcb_status(lcb_logger_callback(logger, Some(logger_callback)))?;
            check_lcb_status(lcb_createopts_logger(create_options, logger))?;

            check_lcb_status(lcb_createopts_connstr(
                create_options,
                connection_string.as_ptr(),
                connection_string_len,
            ))?;

            check_lcb_status(lcb_createopts_credentials(
                create_options,
                username.as_ptr(),
                username_len,
                password.as_ptr(),
                password_len,
            ))?;

            check_lcb_status(lcb_create(&mut inner, create_options))?;
            check_lcb_status(lcb_createopts_destroy(create_options))?;

            Self::install_instance_callbacks(inner);

            lcb_set_cookie(inner, Box::into_raw(instance_cookie) as *const c_void);

            check_lcb_status(lcb_connect(inner))?;
            check_lcb_status(lcb_wait(inner, lcb_WAITFLAGS_LCB_WAIT_DEFAULT))?;
        }

        Ok(Self { inner })
    }

    /// Installs all the operation callbacks from libcouchbase.
    ///
    /// Libcouchbase communicates the results of requests through callbacks defined
    /// on the instance. So when a new instance is created all callbacks need to be
    /// associated with it.
    unsafe fn install_instance_callbacks(instance: *mut lcb_INSTANCE) {
        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_GET as i32,
            Some(get_callback),
        );

        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_STORE as i32,
            Some(store_callback),
        );
        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_EXISTS as i32,
            Some(exists_callback),
        );
        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_REMOVE as i32,
            Some(remove_callback),
        );
        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_SDMUTATE as i32,
            Some(mutate_in_callback),
        );
        lcb_install_callback(
            instance,
            lcb_CALLBACK_TYPE_LCB_CALLBACK_SDLOOKUP as i32,
            Some(lookup_in_callback),
        );
        lcb_set_open_callback(instance, Some(open_callback));
    }

    /// Returns true if there is at least one oustanding request.
    pub fn has_outstanding_requests(&self) -> bool {
        let instance_cookie = unsafe {
            let instance_cookie_ptr: *const c_void = lcb_get_cookie(self.inner);
            Box::from_raw(instance_cookie_ptr as *mut InstanceCookie)
        };
        let outstanding = instance_cookie.has_outstanding();
        Box::into_raw(instance_cookie);
        outstanding
    }

    fn increment_outstanding_requests(&mut self) {
        let mut instance_cookie = unsafe {
            let instance_cookie_ptr: *const c_void = lcb_get_cookie(self.inner);
            Box::from_raw(instance_cookie_ptr as *mut InstanceCookie)
        };
        instance_cookie.increment_outstanding();
        Box::into_raw(instance_cookie);
    }

    /// Makes progress on the instance without blocking.
    pub fn tick_nowait(&mut self) -> Result<(), lcb_STATUS> {
        check_lcb_status(unsafe { lcb_tick_nowait(self.inner) })
    }

    pub fn bind_to_bucket(&mut self, name: String) -> Result<(), lcb_STATUS> {
        debug!("Starting bucket bind for {}", &name);
        let (name_len, c_name) = into_cstring(name.clone());
        unsafe {
            check_lcb_status(lcb_open(self.inner, c_name.as_ptr(), name_len))?;
            check_lcb_status(lcb_wait(self.inner, lcb_WAITFLAGS_LCB_WAIT_DEFAULT))?;
        }
        debug!("Finished bucket bind for {}", &name);
        Ok(())
    }

    pub fn handle_request(&mut self, request: Request) {
        encode_request(self.inner, request);
        self.increment_outstanding_requests();
    }
}

impl Drop for LcbInstance {
    fn drop(&mut self) {
        unsafe {
            lcb_wait(self.inner, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);
            lcb_destroy(self.inner);
        }
    }
}

pub fn decrement_outstanding_requests(instance: *mut lcb_INSTANCE) {
    let mut instance_cookie = unsafe {
        let instance_cookie_ptr: *const c_void = lcb_get_cookie(instance);
        Box::from_raw(instance_cookie_ptr as *mut InstanceCookie)
    };
    instance_cookie.decrement_outstanding();
    Box::into_raw(instance_cookie);
}

/// A stateful cookie associated with a single instance.
///
/// This cookie is available everywhere the instance is used, so it can
/// be used to track instance-global state.
#[derive(Debug)]
struct InstanceCookie {
    outstanding: usize,
}

impl InstanceCookie {
    pub fn new() -> Self {
        Self { outstanding: 0 }
    }

    pub fn increment_outstanding(&mut self) {
        self.outstanding += 1
    }

    pub fn decrement_outstanding(&mut self) {
        self.outstanding -= 1
    }

    pub fn has_outstanding(&self) -> bool {
        self.outstanding > 0
    }
}

/// Manages a collection of `LcbInstance` for multiplexing purposes.
///
/// Each libcouchbase `lcb_insstance` can only handle a single bucket at a time.
/// In order to handle multiple, we need to multiplex them in rust so that the
/// higher level API can use as many as it needs.
#[derive(Default)]
pub struct LcbInstances {
    // The global (gcccp, unbound) instance if present
    global: Option<LcbInstance>,
    // All the instances that are already bound to a bucket
    bound: HashMap<String, LcbInstance>,
}

impl LcbInstances {
    pub fn set_unbound(&mut self, instance: LcbInstance) {
        self.global = Some(instance);
    }

    pub fn set_bound(&mut self, bucket: String, instance: LcbInstance) {
        self.bound.insert(bucket, instance);
    }

    pub fn has_unbound_instance(&self) -> bool {
        self.global.is_some()
    }

    pub fn bind_unbound_to_bucket(&mut self, bucket: String) -> Result<(), lcb_STATUS> {
        let mut instance = self.global.take().unwrap();
        instance.bind_to_bucket(bucket.clone())?;
        self.set_bound(bucket, instance);
        Ok(())
    }

    pub fn have_outstanding_requests(&self) -> bool {
        if let Some(i) = &self.global {
            if i.has_outstanding_requests() {
                return true;
            }
        }

        for i in self.bound.values() {
            if i.has_outstanding_requests() {
                return true;
            }
        }

        false
    }

    pub fn handle_request(&mut self, request: IoRequest) -> Result<bool, lcb_STATUS> {
        match request {
            IoRequest::Data(r) => {
                let instance = match r.bucket() {
                    Some(b) => self.bound.get_mut(b),
                    None => {
                        if self.global.is_some() {
                            self.global.as_mut()
                        } else {
                            self.bound.values_mut().nth(0)
                        }
                    }
                };
                match instance {
                    Some(i) => i.handle_request(r),
                    None => panic!("Could not find open bucket or global bucket!"),
                };
            }
            IoRequest::Shutdown => return Ok(true),
            IoRequest::OpenBucket {
                name,
                connection_string,
                username,
                password,
            } => {
                if self.has_unbound_instance() {
                    self.bind_unbound_to_bucket(name)?
                } else {
                    let mut instance = LcbInstance::new(connection_string, username, password)?;
                    instance.bind_to_bucket(name.clone())?;
                    self.set_bound(name, instance);
                }
            }
        };
        Ok(false)
    }

    pub fn tick_nowait(&mut self) -> Result<(), lcb_STATUS> {
        if let Some(i) = &mut self.global {
            i.tick_nowait()?;
        }

        for i in self.bound.values_mut() {
            i.tick_nowait()?;
        }

        Ok(())
    }
}

#[allow(non_upper_case_globals)]
fn check_lcb_status(status: lcb_STATUS) -> Result<(), lcb_STATUS> {
    match status {
        lcb_STATUS_LCB_SUCCESS => Ok(()),
        _ => Err(status),
    }
}
