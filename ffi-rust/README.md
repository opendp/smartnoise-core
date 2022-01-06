**Notice**: SmartNoise-Core is deprecated. Please migrate to the OpenDP library:
- [OpenDP Rust Crate](https://crates.io/crates/opendp)
- [OpenDP GitHub Repo](https://github.com/opendp/opendp/)

----------------------------------------------------

## SmartNoise Core <br/> Differential Privacy Library FFI <br/>

The FFI interface is a sub-project of [SmartNoise-Core](https://github.com/opendp/smartnoise-core).
See also the accompanying [SmartNoise SDK repository](https://github.com/opendp/smartnoise-sdk) and [SmartNoise-Samples](https://github.com/opendp/smartnoise-samples) repositories for this system.

##

This library provides an FFI interface for the `smartnoise_validator` and `smartnoise_runtime` crates. Refer to those crates for relevant documentation.  

Different combinations of feature flags may be set.


To build with the runtime and secure noise:

    cargo build

To build without the runtime:

    cargo build --no-default-features

To build with the runtime and secure noise:

    cargo build --no-default-features --features use-mpfr

To build with the runtime and secure noise, where GMP and MPFR are provided by the operating system:

    cargo build --no-default-features --features use-system-libs

To build with the runtime, without secure noise:

    cargo build --no-default-features --features use-runtime

*WARNING: do not publish releases using a library build without secure noise*
