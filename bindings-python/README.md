## Install
1. install dependencies  
    
    
    python3 -m pip install -r bindings-python/requirements.txt

2. install yarrow (rerun when binaries or proto are updated)


    python3 setup.py develop


The `setup.py` is used for building/installing the package, under the assumption that protoc has already run.
On Windows, be sure to manually build the protobufs before calling `setup.py`.
