# Install
Use the validator-c++ instructions. Same dependencies/commands.
The c++ runtime has dependencies on the validator-c++/CMakeLists.txt- so the C++ validator must be built, as well as the runtime, to get a usable runtime build.

The validator-C++ CMakeLists produces a separate code-gen protobuf library, and the runtime makes use of functions within the validator.
