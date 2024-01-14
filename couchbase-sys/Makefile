binding:
	@echo Generating binding for version "${VERSION}"
	git clone https://github.com/couchbase/libcouchbase.git libcouchbase-${VERSION}
	cd libcouchbase-${VERSION} && git checkout $(VERSION)
	echo "#include \"libcouchbase-${VERSION}/include/libcouchbase/couchbase.h\"\n\
		#include \"libcouchbase-${VERSION}/include/libcouchbase/"