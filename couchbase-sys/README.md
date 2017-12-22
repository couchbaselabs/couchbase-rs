# Libcouchbase FFI Bindings
Most of the time you want to use the `couchbase` crate directly, here you'll mostly find low level
and contributor information.

This binding will look with pkg-config for a libcouchbase and use a pre-built binding if possible.
If thats not possible two features can be used together or independently:

 - `build-lcb` builds the lcb version which is currently defined in the build file and use that
   one going forward.
- `generate-binding` pulls in more deps and will generate the binding either from the built lcb
   or the found one through pkg-config instead of using a prebuilt one.

## Howto: Rebuilding the bindings.rs for a new libcouchbase version
First, make sure you have `bindgen` installed as a command line utility:

Its important to use the same version as define din the `Cargo.toml` dependency, since bindgen itself changes its output from version to version leading to incompatibilities.

```
cargo install -f bindgen -vers 0.30.0
```

Next, you need `make` and `wget` and then the makefile will do the rest.

```
couchbase-rs/couchbase-sys$ make binding VERSION=2.8.4
```

This will download the lcb source from github and create the binding, putting it into the `src`
directory.
