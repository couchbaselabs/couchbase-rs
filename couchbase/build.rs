#[cfg(feature = "libcouchbase")]
fn main() {
    cc::Build::new().file("src/io/utils.c").compile("utils")
}

#[cfg(not(feature = "libcouchbase"))]
fn main() {}
