#!/usr/bin/env bash
set -e

FILE_MAKEVARS=src/Makevars

# use the external tar program from the operating system to avoid billions of path length warnings
export R_BUILD_TAR=tar

echo "> Copying packages package"

refresh_rust_sources () {

  if test -f "src/$1-rust/Cargo.toml"; then
    echo "RUST: $1 rebuild source"
    # only update src, for faster builds
    rm -r src/$1-rust/src
    cp -fR ../$1-rust/src src/$1-rust/src

  else
    echo "RUST: $1 rebuild all"

    rm -rf src/$1-rust
    cp -fR ../$1-rust/ src/$1-rust

    # default config is to build a dynamic library
    # but `ldd src/whitenoise.so` (the shim) shows the broken (not found) runtime dependency
    # fixed by compiling the runtime statically into the shim
    # TODO: the '' argument was added for mac, may break linux distros
    sed -i '' 's/cdylib/staticlib/g' src/$1-rust/Cargo.toml
  fi
}

refresh_rust_sources "validator"
refresh_rust_sources "runtime"

mkdir -p inst
rm -r inst/prototypes
cp -fR ../prototypes/ inst/prototypes

# REBUILD MAKEVARS FILE
rm -f $FILE_MAKEVARS

# if release is passed to install.sh, then pass build flag to cargo and adjust output directory
RUST_BUILD_TARGET=debug
if [[ -n "$WN_RELEASE" ]] && [[ "$WN_RELEASE" != "false" ]]; then
  RUST_BUILD_TARGET=release
  RUST_BUILD_FLAG=--release
fi

if [[ -n "$WN_USE_SYSTEM_LIBS" ]] && [[ "$WN_USE_SYSTEM_LIBS" != "false" ]]; then
  RUST_FEATURE_FLAG="--features use-system-libs"
fi

cat << EOF > ${FILE_MAKEVARS}
export WN_PROTO_DIR = ../../inst/prototypes
LIBDIR_VALIDATOR = validator-rust/target/${RUST_BUILD_TARGET}
LIBDIR_RUNTIME = runtime-rust/target/${RUST_BUILD_TARGET}

STATLIB_VALIDATOR = \$(LIBDIR_VALIDATOR)/whitenoise_validator.a
STATLIB_RUNTIME = \$(LIBDIR_RUNTIME)/whitenoise_runtime.a

PKG_LIBS = -L\$(LIBDIR_VALIDATOR) -l"whitenoise_validator" -L\$(LIBDIR_RUNTIME) -l"whitenoise_runtime"
# -lresolv -lcrypto -lssl

\$(SHLIB): \$(STATLIB_VALIDATOR) \$(STATLIB_RUNTIME)

\$(STATLIB_VALIDATOR):
	cargo build ${RUST_BUILD_FLAG} --manifest-path=validator-rust/Cargo.toml

\$(STATLIB_RUNTIME):
	cargo build ${RUST_BUILD_FLAG} --manifest-path=runtime-rust/Cargo.toml ${RUST_FEATURE_FLAG}

EOF

#clean:
#	rm -Rf $(SHLIB) $(STATLIB) $(OBJECTS) runtime-rust/target

echo "> Building and installing package"
R -e "pkgbuild::compile_dll(); devtools::document(); devtools::install()"
