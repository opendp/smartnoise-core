#!/usr/bin/env bash
set -e -x

# Rust documentation
# current working directory must be in `whitenoise-core` root.
doc_command="cargo doc --verbose --target-dir=docs --manifest-path=ffi-rust/Cargo.toml"

if [[ "${WN_USE_VULNERABLE_NOISE:-false}" != "false" ]]; then
  doc_command+=" --no-default-features --features use-runtime"
elif [[ "${WN_USE_SYSTEM_LIBS:-false}" != "false" ]]; then
  doc_command+=" --features use-system-libs"
fi

eval "$doc_command"

rm -rf docs/debug;
