## R Install
1. RProtoBuf requires the protobuf libraries to be installed. This project uses protobuf version 3.9.1.  
    <!-- Documentation from https://github.com/eddelbuettel/rprotobuf -->
    Ubuntu instructions:
    
    <!-- Running this installs protobuf 3.0.0, which is not supported -->
    <!-- sudo apt-get install protobuf-compiler libprotobuf-dev libprotoc-dev -->
        https://askubuntu.com/a/1072684

2. Run the install script from this directory


    . install.sh 



## Debugging

Rebuild the burdock.so

    pkgbuild::compile_dll()

To regenerate documentation, update namespace exports, etc with roxygen.

    devtools::document()

To create a built package file

    devtools::build()