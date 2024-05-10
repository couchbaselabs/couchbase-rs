# Download the lcb source from github, putting it into the src directory

## Nest `libcouchbase` inside `couchbase-sys`

```
git subtree add --prefix couchbase-sys/libcouchbase https://github.com/couchbase/libcouchbase.git <branch-name> --squash
```
(The common practice is to not store the entire history of the subproject in your main repository, but If you want to preserve it just omit the –squash flag.)

**_The above command must be executed once._**

## Update the code of the plugin from the upstream repository 

```
git subtree pull --prefix couchbase-sys/libcouchbase https://github.com/couchbase/libcouchbase.git <branch-name> --squash
```