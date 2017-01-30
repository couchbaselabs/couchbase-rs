extern crate bindgen;
extern crate pkg_config;
extern crate cmake;

use std::env;
use std::path::Path;

fn main() {
    let lcb_dir = "libcouchbase-2.7.0";
    let out_dir = env::var("OUT_DIR").unwrap();

    // Step 1: Either use pkg-config or compile from source and link the library

    let bindgen_path = if env::var("COUCHBASE_SYS_USE_PKG_CONFIG").is_ok() {
        let result = pkg_config::find_library("libcouchbase").unwrap();
        String::from(result.include_paths
            .get(0)
            .expect("Could not find include path from pkg config")
            .to_str()
            .unwrap())
    } else {
        let dst = cmake::build(lcb_dir);
        println!("cargo:rustc-link-search=native={}",
                 dst.join("lib").display());
        println!("cargo:rustc-link-lib=dylib=couchbase");
        format!("{}/include", out_dir)
    };

    // Step 2: From the headers, generate the rust binding via bindgen

    let _ = bindgen::builder()
        .header(format!("{}/libcouchbase/couchbase.h", bindgen_path))
        .clang_arg("-I")
        .clang_arg(bindgen_path)
        .no_unstable_rust()
        .generate_comments(false)
        .generate()
        .unwrap()
        .write_to_file(Path::new(&out_dir).join("bindings.rs"));
}
