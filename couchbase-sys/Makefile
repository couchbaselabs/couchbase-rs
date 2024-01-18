binding:
	@echo Generating binding for version "${VERSION}"
	git clone https://github.com/couchbase/libcouchbase.git libcouchbase-${VERSION}
	cd libcouchbase-${VERSION} && git checkout $(VERSION)
	echo "#include \"libcouchbase-${VERSION}/include/libcouchbase/couchbase.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/plugins/io/bsdio-inl.c\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/plugins/io/wsaerr-inl.c\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/plugins/io/wsaerr.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/assert.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/iops.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/auth.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/cntl-private.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/cntl.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/configuration.h.in\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/crypto.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/error.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/iometrics.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/iops.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/ixmgmt.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/kvbuf.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/logger.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/metrics.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/pktfwd.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/sysdefs.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/tracing.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/utils.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/vbucket.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/visibility.h\"" > headers-${VERSION}.h
	bindgen --verbose --no-unstable-rust --no-doc-comments headers-${VERSION}.h -- -I libcouchbase-${VERSION}/include/libcouchbase/ > src/bindings-${VERSION}.rs
	rm -rf libcouchbase-${VERSION}/
	rm headers-${VERSION}.h