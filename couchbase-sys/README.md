# Libcouchbase FFI Bindings
Most of the time you want to use the `couchbase` crate directly, here you'll mostly find low level
and contributor information.

## Howto: Rebuilding the bindings.rs for a new libcouchbase version
First, make sure you have `bindgen` installed as a command line utility:

```
cargo install -f bindgen
```

Next, download the libcouchbase version you want to generate a binding for and unpack it:

```
wget https://github.com/couchbase/libcouchbase/archive/2.7.4.tar.gz
tar -xvzpf 2.7.4.tar.gz
rm 2.7.4.tar.gz
```

You should now have a directory called `libcouchbase-2.7.4`.
