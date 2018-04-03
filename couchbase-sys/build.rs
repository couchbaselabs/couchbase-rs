//! Build script for `couchbase-sys` to bind to `libcouchbase`.
extern crate bindgen;
#[cfg(feature = "build-lcb")]
extern crate cmake;
extern crate pkg_config;

use std::env;

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

fn main() {
    let version = String::from("2.8.5");
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
    let _ = bindgen::builder()
        .header(format!("headers-{}.h", &version))
        .clang_arg("-I")
        .clang_arg(bindgen_path)
        .generate_comments(false)
        .generate()
        .unwrap()
        .write_to_file(std::path::Path::new(&out_dir).join("bindings.rs"));
}
