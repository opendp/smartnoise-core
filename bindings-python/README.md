## Install
1. install dependencies  
    
    
    python -m pip install -r bindings-python/requirements.txt

2. install burdock (rerun when binaries or proto are updated)


    . install.sh


The `setup.py` is used for building/installing the package, under the assumption that protoc has already run.
On Windows, be sure to manually build the protobufs before calling `setup.py`.
