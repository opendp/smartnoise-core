from yarrow._native_validator import ffi as ffi_validator, lib as lib_validator
from yarrow._native_runtime import ffi as ffi_runtime, lib as lib_runtime

import json
import ctypes

from . import api_pb2


class ByteBuffer(ctypes.Structure):
    _fields_ = [
        ("len", ctypes.c_uint64),
        ("data", ctypes.POINTER(ctypes.c_uint8))
    ]


def _serialize_proto(proto, ffi):
    serialized = proto.SerializeToString()
    return ffi.new(f"uint8_t[{len(serialized)}]", serialized), len(serialized)


class LibraryWrapper(object):

    def compute_epsilon(self, analysis, release):
        return lib_validator.compute_privacy(
            *_serialize_proto(analysis, ffi_validator),
            *_serialize_proto(release, ffi_validator)
        )

    def validate_analysis(self, analysis):
        return lib_validator.validate_analysis(
            *_serialize_proto(analysis, ffi_validator)
        )

    def generate_report(self, analysis, release):
        byte_buffer = lib_validator.generate_report(
            *_serialize_proto(analysis, ffi_validator),
            *_serialize_proto(release, ffi_validator)
        )

        json_string = ffi_runtime.string(byte_buffer.data, byte_buffer.len)

        # TODO: why is ffi returning two extra characters: \n\x10, a newline and data link escape control character?
        json_string = json_string[2:]

        return json.loads(json_string)

    def compute_release(self, dataset, analysis, release):

        byte_buffer = lib_runtime.release(
            *_serialize_proto(dataset, ffi_runtime),
            *_serialize_proto(analysis, ffi_runtime),
            *_serialize_proto(release, ffi_runtime)
        )
        serialized_response = ffi_runtime.buffer(byte_buffer.data, byte_buffer.len)
        return api_pb2.ResultRelease.FromString(serialized_response)
