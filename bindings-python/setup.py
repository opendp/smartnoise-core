from setuptools import setup
import os

# turn on backtraces in rust
os.environ['RUST_BACKTRACE'] = 'full'  # '1'


def build_native(spec):
    build_validator = spec.add_external_build(
        cmd=['cargo', 'build', '--release'],
        path='../validator-rust'
    )

    spec.add_cffi_module(
        module_path='yarrow._native_validator',
        dylib=lambda: build_validator.find_dylib('yarrow_validator', in_path='target/release'),
        header_filename=lambda: build_validator.find_header('api.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )

    build_runtime = spec.add_external_build(
        cmd=['cargo', 'build', '--release'],
        path='../runtime-rust'
    )

    spec.add_cffi_module(
        module_path='yarrow._native_runtime',
        dylib=lambda: build_runtime.find_dylib('yarrow_runtime', in_path='target/release'),
        header_filename=lambda: build_runtime.find_header('api.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )


setup(
    name='yarrow',
    version='0.1.0',
    packages=['yarrow'],
    zip_safe=False,
    platforms='any',
    setup_requires=['milksnake'],
    install_requires=['milksnake'],
    milksnake_tasks=[
        build_native
    ]
)
