# Changelog

## 1.0.0-alpha.5 (In Progress)

### Enhancements

 - Update libcouchbase to 3.0.5

### Fixes

 - Make sure libcouchbase gets to run bg tasks every 100ms on
   idle systems

## 1.0.0-alpha.4

### Enhancements

 - Libcouchbase can now be built statically through a feature flag
 - Depend on libcouchbase 3.0.3
 - Libcouchbase is now built with OpenSSL
 - Add the `bucket_name` to `MutationToken`
 - Adds basic support for subdoc operations
 - Add support for multiple buckets to be open at the same time
 - Accessors to cas/mutation token for the `MutationResult`
 - Add various API commands like `ping` and the `UserManager`

### Fixes

 - Stability improvements around dispatching

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