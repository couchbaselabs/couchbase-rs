# Libcouchbase FFI Bindings
Most of the time you want to use the `couchbase` crate directly, here you'll mostly find low level
and contributor information.

This binding will look with pkg-config for a libcouchbase and use a pre-built binding if possible.
If thats not possible and if the `build-lcb` feature is enabled it will try to build it from source.

Note that `build-lcb` is enabled by default so most of the time it should "just work". Please file an 
issue if you are having problems getting the ffi binding to work on your platform.

The only external dependency for building needed is **clang** see https://rust-lang-nursery.github.io/rust-bindgen/requirements.html for more info.