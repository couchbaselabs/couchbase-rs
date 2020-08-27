#![doc(html_root_url = "https://docs.rs/couchbase-sys/1.0.0-alpha.5")]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

#[cfg(feature = "link-static")]
#[link(name = "openssl", kind = "static")]
extern crate openssl_sys;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
