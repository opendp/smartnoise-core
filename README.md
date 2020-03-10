## WhiteNoise Core Differential Privacy
The WhiteNoise Core is built around a data representation for a statistical analysis. There are three types of projects:
- Validator: validates that an analysis is differentially private
- Runtime: execute analysis
- Bindings: helpers to create analysis

The runtime and bindings may be written in any language. The core data representation is in protobuf, and the validator is written in Rust. All projects implement protobuf code generation, protobuf serialization/deserialization, communication over FFI, handle distributable packaging, and have at some point compiled cross-platform (more testing needed). All projects communicate via proto definitions from the `prototypes` directory.  

Alternative C++ and Haskell runtimes and validator stubs have been moved to the `architecture-exploration` branch.  


#### Validator
The rust validator compiles to binaries that expose C foreign function interfaces and read/automatically generate code for protobuf. A validator C FFI is described in the wiki.  

#### Runtimes
The Rust runtime uses a package called ndarray, which feels somewhat like writing numpy in Rust.  

#### Bindings
There are two language bindings, one in Python, one in R. Both support building binaries into an installable package.  

The Python package is more developed, with helper classes, syntax sugar for building analyses, and visualizations.  

The R package uses a shim library in C to interface with compiled binaries. There isn't a programmer interface like in Python yet, but there is a pattern for exposing the C FFI in R code, as well as protobuf generation.  

The steps for adding bindings in a new language are essentially:  
1. set up package management  
2. set up dependency management  
3. pack binaries with the given language's tools  
4. protobuf code generation  
5. FFI implementation and protobuf encoding/decoding  
6. write programmer interface  


### Install
1. Clone the repository  

    git clone $REPOSITORY_URI
  
2. Install Rust

    Mac, Linux:
    
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

3. Install protobuf compiler  
    Mac:  

        brew install protobuf  
        
    Ubuntu:  

        sudo snap install protobuf --classic  

    Windows:  
* download and run installer, accepting defaults, then add /bin directories to PATH
  + Git
  + 7z
  + CMake
  + Mingw-w64
  + Visual Studio Code
* get source for protobuf
  + `git clone https://github.com/protocolbuffers/protobuf/protobuf.git`
  + `cd protobuf`
  + `git submodule update --init --recursive`
* open the code in integrated developer environment
  + launch Windows command prompt
  + `cd path_to_cloned_protobuf_repo`
  + `code .`
* OPTIONAL: configure build in Visual Studio Code
  + https://code.visualstudio.com/docs/cpp/config-mingw#_create-hello-world
  + follow tutorial to build a C++ Hello World, compiled with Mingw-w64
  + Terminal > Configure Default Build Task > C/C++: g++.exe build active file
  + Terminal > Tasks: Run Build Task
* configure and build protobuf
  + `cd path_to_cloned_protobuf_repo\cmake`
  + `mkdir build\release & cd build\release`
  + invoke CMake to create and build the Makefile artifacts
  + `cmake -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=..\..\..\install -DCMAKE_C_COMPILER=gcc.exe -DCMAKE_CXX_COMPILER=g++.exe ..\..`
* compile and test protobuf
  + `cd path_to_cloned_protobuf_repo\cmake\build\release`
  + `mingw32-make all`


4. Install instructions for the bindings, validator and runtime are located in their respective folders.  
