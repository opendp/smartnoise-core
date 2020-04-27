#!/usr/bin/env bash

# Rust documentation
cargo doc --verbose --target-dir=docs --manifest-path=runtime-rust/Cargo.toml
rm -rf docs/debug;
