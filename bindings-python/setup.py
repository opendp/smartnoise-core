from setuptools import setup


def build_native(spec):
    # build an example rust library
    build = spec.add_external_build(
        cmd=['cargo', 'build', '--release'],
        path='../runtime-rust'
    )

    spec.add_cffi_module(
        module_path='burdock._native',
        dylib=lambda: build.find_dylib('differential_privacy_runtime_rust', in_path='target/release'),
        header_filename=lambda: build.find_header('api.h', in_path='.'),
        rtld_flags=['NOW', 'NODELETE']
    )


setup(
    name='burdock',
    version='0.1.0',
    packages=['burdock'],
    zip_safe=False,
    platforms='any',
    setup_requires=['milksnake'],
    install_requires=['milksnake'],
    milksnake_tasks=[
        build_native
    ]
)
