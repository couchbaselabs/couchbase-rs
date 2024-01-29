binding:
	@echo Generating binding for version "3.3.11"
	# Move any existing libcouchbase directories
	mkdir -p backup_libcouchbase
	mv libcouchbase-* backup_libcouchbase/ || true
	#
	wget -N https://github.com/couchbase/libcouchbase/archive/3.3.11.tar.gz
	tar -xvzpf 3.3.11.tar.gz
	# Create header file
	echo "#include \"libcouchbase-3.3.11/include/libcouchbase/couchbase.h\"\n..." > headers-3.3.11.h
	# Generate Rust bindings
	bindgen --verbose --no-unstable-rust --no-doc-comments headers-3.3.11.h -- -I libcouchbase-3.3.11/include/libcouchbase/ > src/bindings-3.3.11.rs
	# Cleanup
	rm -rf 3.3.11.tar.gz libcouchbase-3.3.11 headers-3.3.11.h