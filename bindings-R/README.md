## R Install
Run the install script from this directory 
    Windows: `install.bat`
    Else: `install.sh`

Windows installation is not functional and needs significant debugging.

## Debugging

Rebuild the whitenoise.so

    pkgbuild::compile_dll()

To regenerate documentation, update namespace exports, etc with roxygen.

    devtools::document()

To create a built package file

    devtools::build()
    
To reinstall the package

    devtools::install()
    
Be sure to set the environment variable export `R_BUILD_TAR=tar` for faster compilation and to avoid copious path length warnings.
