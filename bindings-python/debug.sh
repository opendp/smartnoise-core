export QT_XKB_CONFIG_ROOT=/usr/share/X11/xkb
export RUST_BACKTRACE=1

python3 setup.py develop |& tee debug_build.log
python3 -m pytest -x -v |& tee debug_tests.log
python3 app.py |& tee debug_app.log
exit
