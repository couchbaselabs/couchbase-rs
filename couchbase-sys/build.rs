#[cfg(feature = "generate-binding")]
extern crate bindgen;
extern crate pkg_config;
#[cfg(feature = "build-lcb")]
extern crate cmake;

use std::env;
use std::fs;



#[cfg(feature = "build-lcb")]
fn build_lcb(lcb_dir: &str, out_dir: &str) -> String {
    let dst = cmake::build(lcb_dir);
    println!("cargo:rustc-link-search=native={}",
             dst.join("lib").display());
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
        .no_unstable_rust()
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
    let mut version = String::from("2.7.5");
    let lcb_dir = format!("libcouchbase-{}", version);
    let out_dir = env::var("OUT_DIR").unwrap();

    let mut bindgen_path = None;
    let pkg_success = if cfg!(feature = "build-lcb") {
        false
    } else {
        let result = pkg_config::Config::new().atleast_version(&version).probe("libcouchbase");
        if result.is_ok() {
            bindgen_path = Some(String::from(result.as_ref()
                .unwrap()
                .include_paths
                .get(0)
                .expect("Could not find include path from pkg config")
                .to_str()
                .unwrap()));
            version = result.unwrap().version;
            true
        } else {
            false
        }
    };

    if !pkg_success {
        if cfg!(feature = "build-lcb") {
            bindgen_path = Some(build_lcb(&lcb_dir, &out_dir))
        } else {
            panic!("Need to build libcouchbase (none found), but the 'build-lcb' feature is not \
                    enabled!");
        }
    }

    // Step 2: From the headers, generate the rust binding via bindgen or load the pre-gen one
    if cfg!(feature = "generate-binding") {
        match bindgen_path {
            Some(bp) => generate_binding(&bp, &out_dir, &version),
            None => panic!("Instructed to generate binding, but no path for headers found."),
        }
    } else {
        let src_path = format!("{}/src/bindings-{}.rs",
                               env!("CARGO_MANIFEST_DIR"),
                               &version);
        if !std::path::Path::new(&src_path).exists() {
            panic!("No binding found for libcouchbase version {} in the Rust SDK source",
                   version);
        }
        let dst_path = format!("{}/bindings.rs", out_dir);
        fs::copy(&src_path, &dst_path).unwrap();
    }
}
