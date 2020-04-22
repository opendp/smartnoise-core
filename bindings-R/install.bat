rem FILE NOT TESTED ON WINDOWS YET

set FILE_RUST_MANIFEST=runtime-rust/Cargo.toml
set FILE_MAKEVARS=src/Makevars.win

echo "> Copying runtime package"

if exist src\%FILE_RUST_MANIFEST% (

    echo "RUST: rebuild source"
    rem only update src, for faster builds
    del /Q /S src\runtime-rust\src\*.*

    xcopy /s ..\runtime-rust\src src\runtime-rust\src

) else (
    echo "RUST: rebuild all"
    del /Q /S src\runtime-rust\*.*
    xcopy /s ..\runtime-rust\ src\runtime-rust

    del /Q /S src\prototypes\*.*
    xcopy /s ..\prototypes\ src\prototypes

    rem runtime's default config is to build a dynamic library
    rem but `ldd src\whitenoise.so` (the shim) shows the broken (not found) runtime dependency
    rem fixed by compiling the runtime statically into the shim
    powershell -Command "(gc src\runtime-rust\Cargo.toml) -replace 'dylib', 'staticlib' | Out-File -encoding ASCII src\runtime-rust\Cargo.toml"
)

rem REBUILD MAKEVARS FILE

del %FILE_MAKEVARS%

rem if release is passed to install.sh, then pass build flag to cargo and adjust output directory
if %1 == "release" (
    set RUST_BUILD_FLAG=--release
    set RELEASE_MODE=%1
) else (
    set RELEASE_MODE=debug
)

(
    echo TARGET = $(subst 64,x86_64,$(subst 32,i686,$(WIN)))-pc-windows-gnu
    echo LIBDIR = runtime-rust/target/%RELEASE_MODE%
    echo STATLIB = $(LIBDIR)/libdifferential_privacy_runtime_rust.lib
    echo PKG_LIBS = -L$(LIBDIR) -l"differential_privacy_runtime_rust" -lws2_32 -ladvapi32 -luserenv -lcrypto -lssl
    echo
    echo
    echo $(SHLIB): $(STATLIB)
    echo
    echo $(STATLIB):
    echo 	cargo +nightly build %RUST_BUILD_FLAG%--manifest-path=%FILE_RUST_MANIFEST%
) > %FILE_MAKEVARS%

echo "> Building and installing package"
cd ..
R -e "devtools::install('bindings-R')"
