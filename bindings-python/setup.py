from setuptools import setup
import os

# turn on backtraces in rust (for build.rs)
os.environ['RUST_BACKTRACE'] = 'full'  # '1'
os.environ['RUSTFLAGS'] = ""

release = False

rust_build_path = 'target/' + ('release' if release else 'debug')
rust_build_cmd = ['cargo', 'build']

if release:
    rust_build_cmd.append('--release')


def build_native(spec):
    build_validator = spec.add_external_build(
        cmd=rust_build_cmd,
        path='../validator-rust'
    )

    spec.add_cffi_module(
        module_path='whitenoise._native_validator',
        dylib=lambda: build_validator.find_dylib('whitenoise_validator', in_path=rust_build_path),
        header_filename=lambda: build_validator.find_header('api.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )

    build_runtime = spec.add_external_build(
        cmd=rust_build_cmd,
        path='../runtime-rust'
    )

    spec.add_cffi_module(
        module_path='whitenoise._native_runtime',
        dylib=lambda: build_runtime.find_dylib('whitenoise_runtime', in_path=rust_build_path),
        header_filename=lambda: build_runtime.find_header('api.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )


def build_python(spec):
    spec.add_external_build(
        cmd=['python3', 'code_generation.py'],
        path="."
    )


setup(
    name='whitenoise',
    version='0.1.0',
    packages=['whitenoise'],
    zip_safe=False,
    platforms='any',
    setup_requires=['milksnake'],
    install_requires=['milksnake'],
    milksnake_tasks=[
        build_native,
        build_python
    ]
)
