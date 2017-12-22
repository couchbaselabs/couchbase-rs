//! Build script for `couchbase-sys` to bind to `libcouchbase`.
//!
//! The way we pick up lcb has undergone a few different approaches,
//! but for now we settled on the following:
//!
//! 1) lcb is looked up via `pkg-config`
//! 2a) if found, go to step 3.
//! 2b) if not found, try to compile it via cmake.
//! 3) if `generate-binding` is enabled (not by default), then
//!    a binding will be generated via bindgen, otherwise by default
//!    we'll pick one from the list of stored ones.
//!
//! Features `build-lcb` is enabled by default. If `build-lcb` and its 
//! not found via pkg-config, building will not work (but it can be used 
//! to make sure its only picked up via pkg-config).
#[cfg(feature = "generate-binding")]
extern crate bindgen;
#[cfg(feature = "build-lcb")]
extern crate cmake;
extern crate pkg_config;

use std::env;
use std::fs;

#[cfg(feature = "build-lcb")]
fn build_lcb(lcb_dir: &str, out_dir: &str) -> String {
    let dst = cmake::build(lcb_dir);
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!("cargo:rustc-link-lib=dylib=couchbase");
    format!("{}/include", out_dir)
}

#[cfg(not(feature = "build-lcb"))]
fn build_lcb(_lcb_dir: &str, _out_dir: &str) -> String {
    unreachable!();
}

#[cfg(feature = "generate-binding")]
fn generate_binding(bindgen_path: &str, out_dir: &str, version: &str) {
    let _ = bindgen::builder()
        .header(format!("headers-{}.h", version))
        .clang_arg("-I")
        .clang_arg(bindgen_path)
        .generate_comments(false)
        .generate()
        .unwrap()
        .write_to_file(std::path::Path::new(out_dir).join("bindings.rs"));
}

#[cfg(not(feature = "generate-binding"))]
fn generate_binding(_bindgen_path: &str, _out_dir: &str, _version: &str) {
    unreachable!();
}

fn main() {
    let version = String::from("2.8.4");
    let lcb_dir = format!("libcouchbase-{}", version);
    let out_dir = env::var("OUT_DIR").unwrap();

    let result = pkg_config::Config::new()
        .atleast_version(&version)
        .probe("libcouchbase");

    let bindgen_path = match result {
        Ok(_) => String::from(
            result
                .as_ref()
                .unwrap()
                .include_paths
                .get(0)
                .expect("Could not find include path from pkg config")
                .to_str()
                .unwrap(),
        ),
        Err(_) if cfg!(feature = "build-lcb") => build_lcb(&lcb_dir, &out_dir),
        Err(_) => panic!(
            "Need to build libcouchbase (none found), but the 'build-lcb' feature is not \
             enabled!"
        ),
    };

    // Step 2: From the headers, generate the rust binding via bindgen or load the pre-gen one
    if cfg!(feature = "generate-binding") {
        generate_binding(&bindgen_path, &out_dir, &version)
    } else {
        let src_path = format!(
            "{}/src/bindings-{}.rs",
            env!("CARGO_MANIFEST_DIR"),
            &version
        );
        if !std::path::Path::new(&src_path).exists() {
            panic!(
                "No binding found for libcouchbase version {} in the Rust SDK source",
                version
            );
        }
        let dst_path = format!("{}/bindings.rs", out_dir);
        fs::copy(&src_path, &dst_path).unwrap();
    }
}
