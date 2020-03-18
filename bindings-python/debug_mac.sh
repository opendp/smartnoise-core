#!/usr/bin/env bash

# enables full stack traces
export RUST_BACKTRACE=1


# rebuilds the validator, runtime, protobuf, components.py and python package
#python3 setup.py develop |& tee debug_build.log
python3 -m pip install -e . -v | tee debug_build.log
# run tests
#python3 -m pytest -x -v | tee debug_tests.log

# run a test application
python3 app.py | tee debug_app.log

exit
