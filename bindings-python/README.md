## Python Install
1. install dependencies  
    

    python -m pip install -r bindings-python/requirements.txt
    
2. build the protobuf for python. On ubuntu, this is:
    
    
    cd ${REPOSITORY_DIR}
    (cd prototypes && protoc --python_out=../bindings-python/ *.proto)
