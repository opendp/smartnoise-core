[![Build Status](https://travis-ci.org/opendifferentialprivacy/whitenoise-core.svg?branch=develop)](https://travis-ci.org/opendifferentialprivacy/whitenoise-core)

## WhiteNoise Core <br/> Differential Privacy Library FFI <br/>

The FFI interface is a sub-project of [Whitenoise-Core](https://github.com/opendifferentialprivacy/whitenoise-core).
See also the accompanying [WhiteNoise-System](https://github.com/opendifferentialprivacy/whitenoise-system) and [WhiteNoise-Samples](https://github.com/opendifferentialprivacy/whitenoise-samples) repositories for this system.

##

This library provides an FFI interface for the `whitenoise_validator` and `whitenoise_runtime` crates. Refer to those crates for relevant documentation.  

Different combinations of feature flags may be set.


To build with the runtime and secure noise:

    cargo build

To build without the runtime:

    cargo build --no-default-features
    
To build with the runtime and secure noise:

    cargo build --no-default-features --features use-secure-noise
    
To build with the runtime and secure noise, where GMP and MPFR are provided by the operating system:

    cargo build --no-default-features --features use-system-libs

To build with the runtime, without secure noise:

    cargo build --no-default-features --features use-runtime

*WARNING: do not publish releases using a library build without secure noise*
