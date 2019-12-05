FILE_RUST_MANIFEST=runtime-rust/Cargo.toml
FILE_MAKEVARS=src/Makevars

echo "> Copying runtime package"

if test -f "src/$FILE_RUST_MANIFEST"; then
  echo "RUST: rebuild source"
  # only update src, for faster builds
  rm -r src/runtime-rust/src
  cp -fR ../runtime-rust/src src/runtime-rust/src
else
  echo "RUST: rebuild all"

  rm -r src/runtime-rust
  cp -fR ../runtime-rust/ src/runtime-rust

  rm -r src/prototypes
  cp -fR ../prototypes/ src/prototypes

  # runtime's default config is to build a dynamic library
  # but `ldd src/yarrow.so` (the shim) shows the broken (not found) runtime dependency
  # fixed by compiling the runtime statically into the shim
  sed -i 's/dylib/staticlib/g' src/runtime-rust/Cargo.toml
fi

# REBUILD MAKEVARS FILE
rm $FILE_MAKEVARS

# if release is passed to install.sh, then pass build flag to cargo and adjust output directory
if [[ $1 == "release" ]]; then
  RUST_BUILD_FLAG=--release
fi

cat << EOF > ${FILE_MAKEVARS}
LIBDIR = runtime-rust/target/${1:-debug}
STATLIB = \$(LIBDIR)/libdifferential_privacy_runtime_rust.a
PKG_LIBS = -L\$(LIBDIR) -l"differential_privacy_runtime_rust" -lresolv -lcrypto -lssl


\$(SHLIB): \$(STATLIB)

\$(STATLIB):
	cargo +nightly build ${RUST_BUILD_FLAG}--manifest-path=${FILE_RUST_MANIFEST}

EOF

#clean:
#	rm -Rf $(SHLIB) $(STATLIB) $(OBJECTS) runtime-rust/target

echo "> Building and installing package"
(cd .. && R -e "devtools::install('bindings-R')")
