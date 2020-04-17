#!/usr/bin/env bash

# Python documentation
cd bindings-python/
pip3 install -r requirements.txt
sphinx-apidoc -fFe -H whitenoise-core -A "Consequences of Data" -V 0.1.0 -o docs/source/ ./whitenoise/ ./whitenoise/*_pb2.py --templatedir templates/

# destroy prior generated documentation and completely rebuild
rm -r ../docs/bindings-python/
sphinx-build -b html ./docs/source/ ../docs/bindings-python/

rm -r ./docs/
cd ..

# Rust documentation
cargo doc --verbose --target-dir=docs --manifest-path=runtime-rust/Cargo.toml
rm -rf docs/debug;
