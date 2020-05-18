If this file does not resolve your install, please open an issue.
  

## If `cargo build` fails due to the crate `gmp-mpfr-sys`
This often manifests as a compilation failure from `cargo build` ending with

    thread 'main' panicked at 'Program failed with code 2: "make" "-j" "12" "check"'


1. The `gmp-mpfr-sys` crate builds `gmp` and `mpfr` C libraries. This process uses libtools, which requires no spaces in the path to your clone.
    Move/rename the path to your clone and try again.

2. Bypass the `gmp-mpfr-sys` crate builds of `gmp` and `mpfr` by installing `gmp` and `mpfr` externally.  
    1. install system libraries (GMP version 6.2, MPFR version 4.0.2-p1)  
        Mac:
        
            brew install gmp mpfr
        
        Linux:  
            Build gmp and mpfr from source. Then set the environment variable:
        
            export DEP_GMP_OUT_DIR=/path/to/folder/containing/lib/and/includes
        
        Windows:  
            This is not fully tested. Build gmp and mpfr from source. Then set the environment variable and also switch the rust toolchain:
        
            setx DEP_GMP_OUT_DIR=/path/to/folder/containing/lib/and/includes
            rustup toolchain install stable-%WN_SYS_ARCH%-pc-windows-gnu
            rustup default stable-%WN_SYS_ARCH%-pc-windows-gnu
    
    2. You will no longer be able to run `cargo build` on the virtual root crate because they have different feature flags. 
        Just build each crate separately.
        
        To build the runtime, set the feature flag
            
            cd runtime-rust; cargo build --feature use-system-libs
        
        When building any of the language bindings, just set the variable
        
            export WN_USE_SYSTEM_LIBS=true

3. Bypass the `gmp-mpfr-sys` crate completely. 
    *WARNING: do not publish releases using a library build without secure noise*   
    This feature is currently only available in the `ms-exponential-4` branch.
    
    To build the runtime, set the feature flag
    
        cd runtime-rust; cargo build --no-default-features
    
    When building any of the language bindings, just set the variable
    
        export WN_USE_VULNERABLE_NOISE=true

## If `cargo build` fails due to the package `openssl`

Provide an alternative openssl installation, either via directions in the automatic or manual section:
  + https://docs.rs/openssl/0.10.29/openssl/

## Windows installation without WSL

    choco install rust msys2 protoc python
    
For non-Chocolatey users: download and install the latest build of rust, msys2, protobuf and python
- https://forge.rust-lang.org/infra/other-installation-methods.html
- https://github.com/protocolbuffers/protobuf/releases/latest
- https://www.msys2.org/
- https://www.python.org/downloads/windows/

Then install gcc under MSYS2
    
    refreshenv
    reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && setx WN_SYS_ARCH=i686 || setx WN_SYS_ARCH=x86_64
    bash -xlc "pacman --noconfirm -S --needed pacman-mirrors"
    bash -xlc "pacman --noconfirm -S --needed diffutils make mingw-w64-%WN_SYS_ARCH%-gcc"
    
You can test with `bash -xc cargo build`. The bash prefix ensures that gmp and mpfr build with the GNU/gcc/mingw toolchain.
