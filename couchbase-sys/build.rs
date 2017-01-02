extern crate libbindgen;
extern crate pkg_config;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    let _ = libbindgen::builder()
        .header("libcouchbase-2.7.0/include/libcouchbase/couchbase.h")
        .no_unstable_rust()
        .generate()
        .unwrap()
        .write_to_file(Path::new(&out_dir).join("bindings.rs"));

    // if env::var("COUCHBASE_SYS_USE_PKG_CONFIG").is_ok() {
    if pkg_config::find_library("libcouchbase").is_ok() {
        return;
    }
    // }

    // TODO: statically compile and link if the flag is set properly...

}
