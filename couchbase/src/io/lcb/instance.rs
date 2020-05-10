use crate::io::lcb::callbacks::*;
use crate::io::lcb::encode::into_cstring;
use crate::io::lcb::{encode_request, IoRequest};
use couchbase_sys::*;
use log::debug;
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
    pub fn tick_nowait(&self) -> Result<(), lcb_STATUS> {
        check_lcb_status(unsafe { lcb_tick_nowait(self.inner) })
    }

    /// Handle the `IoRequest` and either dispatch/encode the op or handle operations
    /// like shutdown or open bucket.
    pub fn handle_request(&mut self, request: IoRequest) -> Result<bool, lcb_STATUS> {
        match request {
            IoRequest::Data(r) => {
                encode_request(self.inner, r);
                self.increment_outstanding_requests();
            }
            IoRequest::Shutdown => return Ok(true),
            IoRequest::OpenBucket { name } => unsafe {
                debug!("Starting bucket open for {}", &name);
                let (name_len, c_name) = into_cstring(name.clone());
                check_lcb_status(lcb_open(self.inner, c_name.as_ptr(), name_len))?;
                check_lcb_status(lcb_wait(self.inner, lcb_WAITFLAGS_LCB_WAIT_DEFAULT))?;
                debug!("Finished bucket open for {}", &name);
            },
        };
        Ok(false)
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
struct LcbInstances {}

#[allow(non_upper_case_globals)]
fn check_lcb_status(status: lcb_STATUS) -> Result<(), lcb_STATUS> {
    match status {
        lcb_STATUS_LCB_SUCCESS => Ok(()),
        _ => Err(status),
    }
}
