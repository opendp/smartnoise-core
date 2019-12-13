
# generate protobuf
(cd ../prototypes && protoc --python_out=../bindings-python/yarrow *.proto)

# fix relative imports
# https://github.com/protocolbuffers/protobuf/issues/1491#issuecomment-438138293
(cd yarrow && sed -i -E 's/^import.*_pb2/from . &/' *.py)

python3 setup.py develop
