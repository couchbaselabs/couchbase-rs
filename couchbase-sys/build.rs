//! Build script for `couchbase-sys` to bind to `libcouchbase`.

use std::env;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    let mut build_cfg = cmake::Config::new("libcouchbase");

    if env::var("PROFILE").unwrap() == "release" {
        build_cfg.define("CMAKE_BUILD_TYPE", "RelWithDebInfo");
    } else {
        build_cfg.define("CMAKE_BUILD_TYPE", "DEBUG");
    }

    build_cfg.define("LCB_NO_TESTS", "ON");
    build_cfg.define("LCB_NO_TOOLS", "ON");
    build_cfg.define("LCB_NO_MOCK", "ON");
    build_cfg.define("LCB_BUILD_LIBEV", "OFF");
    build_cfg.define("LCB_BUILD_LIBUV", "OFF");

    // list of environment flags that control libcouchbase compilation
    // if one you need is not in this list, feel free to add it and
    // submit a PR!
    let env_flags = vec![
        "LIBEVENT_ROOT",
        "OPENSSL_ROOT_DIR",
        "CMAKE_CXX_COMPILER",
        "CMAKE_C_COMPILER",
        "LCB_BUILD_STATIC",
        "BUILD_SHARED_LIBS",
        "LCB_NO_PLUGINS",
        "LCB_USE_ASAN",
        "LCB_USE_COVERAGE",
        "LCB_NO_SSL",
        "LCB_BUILD_LIBEVENT",
        "LCB_BUILD_LIBEV",
        "LCB_BUILD_LIBUV",
        "LIBCOUCHBASE_STATIC",
    ];

    for flag in env_flags.iter().filter(|flag| env::var(flag).is_ok()) {
        build_cfg.define(flag, env::var(flag).unwrap());
    }

    if cfg!(feature = "link-static") {
        build_cfg.define("LCB_BUILD_STATIC", "ON");
        build_cfg.define("LIBCOUCHBASE_STATIC", "ON");
    }

    let build_dst = build_cfg.build();

    if cfg!(feature = "link-static") {
        std::fs::copy(
            format!("{}/libcouchbaseS.a", build_dst.join("build/lib").display()),
            format!("{}/libcouchbase.a", build_dst.join("build/lib").display()),
        )
        .unwrap();

        if cfg!(any(target_os = "macos", target_os = "freebsd")) {
            println!("cargo:rustc-link-lib=dylib=c++");
        } else {
            println!("cargo:rustc-link-lib=dylib=resolv");
            println!("cargo:rustc-link-lib=dylib=stdc++");
            println!("cargo:rustc-link-lib=dylib=gcc");
        }
        println!("cargo:rustc-link-lib=static=couchbase");
    } else {
        println!("cargo:rustc-link-lib=dylib=couchbase");
    }

    println!(
        "cargo:rustc-link-search=native={}",
        build_dst.join("build/lib").display()
    );

    let mut bindings_builder = bindgen::Builder::default()
        .header("headers.h")
        .clang_arg("-I")
        .clang_arg(format!("{}/include", env::var("OUT_DIR").unwrap()))
        .blacklist_type("max_align_t")
        .generate_comments(false);

    if cfg!(feature = "volatile") {
        if cfg!(any(target_os = "macos", target_os = "freebsd")) {
            bindings_builder = bindings_builder.header("internal-headers-osx.h");
        } else {
            bindings_builder = bindings_builder.header("internal-headers-linux.h");
        }
    }

    let bindings = bindings_builder
        .generate()
        .expect("Unable to generate bindings!");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings!");
}
