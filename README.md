## Differential Privacy Proposal

This is a proposal for what a centralized differential privacy library might look like.


## Install
1. Clone this repository `git clone $REPOSITORY_URI`  
2. Install cmake  
    - https://askubuntu.com/a/865294  
    - re-open terminal after installation  
    - ~~sudo apt install cmake~~ is outdated (pinned to the last LTS release)  
3. move into repository `cd $REPOSITORY_PATH`
4. Install dependencies for each project  
```
python -m pip install conan  
. ./dependencies.sh
```

## Debug/Release
1. move into repository `cd $REPOSITORY_PATH`
    - to build all projects, move into root
    - to build a specific project, move into the project folder
2. set build type
    - debug: `DP_BUILD_TYPE=Debug`
    - release: `DP_BUILD_TYPE=Release`
2. build projects
```
cmake -DCMAKE_BUILD_TYPE=${DP_BUILD_TYPE:=Debug} ./ -G "Unix Makefiles"
```
