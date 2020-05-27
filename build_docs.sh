#!/usr/bin/env bash

# Rust documentation
WN_USE_SYSTEM_LIBS="${WN_USE_SYSTEM_LIBS:-false}"

if [[ "$WN_USE_SYSTEM_LIBS" != "false" ]]; then
  WN_FEATURES="--features use-system-libs"
fi

cargo doc --verbose --target-dir=docs --manifest-path=runtime-rust/Cargo.toml $WN_FEATURES

rm -rf docs/debug;
