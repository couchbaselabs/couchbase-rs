extern crate bindgen;
extern crate pkg_config;
extern crate cmake;

use std::env;
use std::path::Path;

fn main() {
    let lcb_dir = "libcouchbase-2.7.0";
    let out_dir = env::var("OUT_DIR").unwrap();
    let target = env::var("TARGET").unwrap();
    let windows = target.contains("windows");

    let _ = bindgen::builder()
        .header(format!("{}/include/libcouchbase/couchbase.h", lcb_dir))
        .no_unstable_rust()
        .generate()
        .unwrap()
        .write_to_file(Path::new(&out_dir).join("bindings.rs"));

    if env::var("COUCHBASE_SYS_USE_PKG_CONFIG").is_ok() {
        if pkg_config::find_library("libcouchbase").is_ok() {
            return;
        }
    }

    if windows {
        panic!("Building from source for windows is not yet supported!");
    }

    let dst = cmake::build(lcb_dir);
    println!("cargo:rustc-link-search=native={}",
             dst.join("lib").display());
    println!("cargo:rustc-link-lib=dylib=couchbase");
}
