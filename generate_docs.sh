#!/usr/bin/env bash

# Python documentation
# cd bindings-python/docs/

# # regenerate apidocs from docstrings
# rm source/modules.rst
# rm source/whitenoise.rst
# sphinx-apidoc -f -o source/ ../whitenoise/ ../whitenoise/*_pb2.py


cd bindings-python/
sphinx-apidoc -fFe -H whitenoise-core -A "Consequences of Data" -V 0.1.0 -o docs/source/ ./whitenoise/ ./whitenoise/*_pb2.py --templatedir templates/

# destroy prior generated documentation and completely rebuild
rm -r ../docs/bindings-python/
sphinx-build -b html ./docs/source/ ../docs/bindings-python/

rm -r ./docs/
cd ..

# Rust documentation
cd runtime-rust/
cargo doc --verbose --target-dir=../docs;
cd ..
rm -rf docs/debug;