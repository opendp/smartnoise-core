
## Install

1. Install cmake (once, optional)  
    - this can be skipped if building with most IDE's, because the IDE bundles CMake  
    - https://askubuntu.com/a/865294  
    - re-open terminal after installation  
    - ~~sudo apt install cmake~~ is outdated (pinned to the last LTS release)
    - install version 3.14 or later

2. Install C++ dependencies (after every conan dependency change)
```
python -m pip install conan  
conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan
. ./dependencies.sh
```

3. Build C++ projects (after every C++ code modification)
    Opening a subproject in an IDE will load the CMakeLists automatically into run configurations.
    NOTE: shared libraries only need to be built, not run- look for artifacts in `cmake-build-debug/lib/`
    NOTE: code completion on protobuf classes are only as current as the last compilation, when their sources are generated

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
