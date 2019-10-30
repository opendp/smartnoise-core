
# generate protobuf
(cd ../prototypes && protoc --python_out=../bindings-python/burdock *.proto)

# fix relative imports
# https://github.com/protocolbuffers/protobuf/issues/1491#issuecomment-438138293
(cd burdock && sed -i -E 's/^import.*_pb2/from . \0/' *.py)

python setup.py develop
