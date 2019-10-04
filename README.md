## Differential Privacy Proposal

This is a proposal for what a centralized differential privacy library might look like.

## Install
1. clone the repository (once)


    git clone $REPOSITORY_URI

2. Install protobuf compiler. For ubuntu, this is:


    sudo snap install protobuf

3. Install cmake (once)  
    - this can be skipped if building with most IDE's, because the IDE bundles CMake  
    - https://askubuntu.com/a/865294  
    - re-open terminal after installation  
    - ~~sudo apt install cmake~~ is outdated (pinned to the last LTS release)

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