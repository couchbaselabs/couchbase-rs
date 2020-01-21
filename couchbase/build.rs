fn main() {
    if cfg!(feature = "libcouchbase") {
        cc::Build::new().file("src/io/utils.c").compile("utils")
    }
}
