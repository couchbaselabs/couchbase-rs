# Changelog

## 1.0.0-alpha.4

### Enhancements

 - Add the `bucket_name` to `MutationToken`

### Fixes


## 1.0.0-alpha.3

### Enhancements

Note: this release another big rewrite but hopefully the last one on the track to a stable release. Much more API needs to be fleshed out, but the one that is in there should not change a lot.

 - Moved to `std::future` and `async/await`.
 - Updated libcouchbase to `3.0.0`.
 - Significantly overhauled the API to be in-line with the "SDK RFC 3.0" initiative.
 - All APIs now return consistent results and take consistent options.
 - Make use of features (for now only to enable libcouchbase by default).

## 1.0.0-alpha.2

### Fixes

 - Fixed a double import bug that slipped into 1.0.0-alpha.1
 
### Enhancements

 - Added SharedCluster and SharedBucket so it can be used in a multithreaded environment.
 - Added support for N1QL positional and named arguments.
 - Added support for Analytics positional and named arguments.

## 1.0.0-alpha.1

This is the first pre-release of the Couchbase Rust SDK 1.0.0, rendering the previous 0.x releases obsolete.

The API has been completely reworked and it is based on `libcouchbase` 3.0.0-alpha.3. Subsequent releases
will contain proper release notes over the changes.