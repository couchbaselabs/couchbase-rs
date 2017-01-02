extern crate libbindgen;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    let _ = libbindgen::builder()
        .header("libcouchbase-2.7.0/include/libcouchbase/couchbase.h")
        //.use_core()
        .no_unstable_rust()
        .generate()
        .unwrap()
        .write_to_file(Path::new(&out_dir).join("bindings.rs"));
}
