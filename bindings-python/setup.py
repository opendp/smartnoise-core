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
    build_rust = spec.add_external_build(
        cmd=rust_build_cmd,
        path='../'
    )

    spec.add_cffi_module(
        module_path='whitenoise._native_validator',
        dylib=lambda: build_rust.find_dylib('whitenoise_validator', in_path=rust_build_path),
        header_filename=lambda: build_rust.find_header('api_validator.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )

    spec.add_cffi_module(
        module_path='whitenoise._native_runtime',
        dylib=lambda: build_rust.find_dylib('whitenoise_runtime', in_path=rust_build_path),
        header_filename=lambda: build_rust.find_header('api_runtime.h', in_path='.'),
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
    extras_require={
        "plotting": [
            "networkx",
            "matplotlib"
        ],
        "test": [
            "pytest>=4.4.2"
        ]
    },
    milksnake_tasks=[
        build_native,
        build_python
    ]
)
