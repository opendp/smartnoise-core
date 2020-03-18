#!/usr/bin/env bash
# fixes matplotlib plotting bug on ubuntu
export QT_XKB_CONFIG_ROOT=/usr/share/X11/xkb

# enables full stack traces
export RUST_BACKTRACE=1


# rebuilds the validator, runtime, protobuf, components.py and python package
#python3 setup.py develop |& tee debug_build.log
#python3 -m pip install -e . |& tee debug_build.log # ubuntu
python3 -m pip install -e . -v | tee debug_build.log # Mac
# run tests
#python3 -m pytest -x -v |& tee debug_tests.log

# run a test application
#python3 app.py |& tee debug_app.log # ubuntu
python3 app.py | tee debug_app.log   # Mac

exit
