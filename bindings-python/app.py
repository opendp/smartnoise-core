import os

if os.name != 'nt':
    # auto-update the protos
    import subprocess

    # auto-recompile proto files when developing
    # protoc must be installed and on path
    package_dir = os.path.join(os.getcwd(), 'burdock')
    subprocess.call(f"protoc --python_out={package_dir} *.proto", shell=True, cwd=os.path.abspath('../prototypes/'))
    subprocess.call(f"sed -i -E 's/^import.*_pb2/from . \\0/' *.py", shell=True, cwd=package_dir)

if os.name == 'nt' and not os.path.exists('burdock/analysis_pb2.py'):
    print('make sure to run protoc to generate python proto bindings, and fix package imports to be relative to burdock')

from tests.test_base import (
    test_basic_path,
    test_haskell_validator,
    test_rust_sampling,
    test_ndarray
)

test_basic_path()
# test_haskell_validator()
# test_rust_sampling()
# test_ndarray()
