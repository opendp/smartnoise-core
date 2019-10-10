## Differential Privacy Proposal

This is a proposal for what a centralized differential privacy library might look like.

## Install
1. clone the repository (once)


    git clone $REPOSITORY_URI

2. Install protobuf compiler from source
    Mac:
        - install xcode `sudo xcode-select --install`
        - install macports https://www.macports.org/install.php
        - install unix make tools `sudo /opt/local/bin/port install autoconf automake libtool`
        - continue on with the Ubuntu install directions
    Ubuntu:
        - download the "all" Github release. Use version 3.9.x (because of conan)
          `https://github.com/protocolbuffers/protobuf/releases/download/v3.9.1/protobuf-all-3.9.1.zip`
        - Follow the Protobuf instructions, starting from `./configure`
          `https://github.com/protocolbuffers/protobuf/blob/master/src/README.md`
          NOTE: move to a directory without spaces in the path


3. Install cmake (once, optional)  
    - this can be skipped if building with most IDE's, because the IDE bundles CMake  
    - https://askubuntu.com/a/865294  
    - re-open terminal after installation  
    - ~~sudo apt install cmake~~ is outdated (pinned to the last LTS release)
    - install version 3.14 or later

4. Install C++ dependencies (after every conan dependency change)
```
python -m pip install conan  
conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan
. ./dependencies.sh
```

5. Build C++ projects (after every C++ code modification)
    Opening a subproject in an IDE will load the CMakeLists automatically into run configurations.  
    Alternatively, to manually build:  
    - move into repository `cd $REPOSITORY_PATH`
        * to build all projects, stay in root
        * to build a specific project, move into the project folder
    - set build type
        * debug: `DP_BUILD_TYPE=Debug`
        * release: `DP_BUILD_TYPE=Release`
    - build projects

```
cmake -DCMAKE_BUILD_TYPE=${DP_BUILD_TYPE:=Debug} ./ -G "Unix Makefiles"
```

6. Build Python bindings (optional)
    - follow instructions in Python README.md