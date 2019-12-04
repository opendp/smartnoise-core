from yarrow._native_validator import ffi as ffi_validator, lib as lib_validator
from yarrow._native_runtime import ffi as ffi_runtime, lib as lib_runtime

import json
import ctypes

from . import release_pb2


class ByteBuffer(ctypes.Structure):
    _fields_ = [
        ("len", ctypes.c_uint64),
        ("data", ctypes.POINTER(ctypes.c_uint8))
    ]


def _serialize_proto(proto, ffi):
    serialized = proto.SerializeToString()
    return ffi.new(f"uint8_t[{len(serialized)}]", serialized), len(serialized)


class LibraryWrapper(object):
    # def __init__(self):
    #     # load validator functions
    #     lib_validator.validate_analysis.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
    #     lib_validator.validate_analysis.restype = ctypes.c_bool
    #
    #     lib_validator.compute_epsilon.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
    #     lib_validator.compute_epsilon.restype = ctypes.c_double
    #
    #     lib_validator.generate_report.argtypes = (
    #         ctypes.c_char_p, ctypes.c_int64,  # input analysis
    #         ctypes.c_char_p, ctypes.c_int64)  # input release
    #     lib_validator.generate_report.restype = ctypes.c_void_p
    #
    #     lib_validator.free_ptr.argtypes = (ctypes.c_void_p,)

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

        # serialized_report = ctypes.cast(serialized_report_ptr, ctypes.c_char_p).value
        # return json.loads(serialized_report)

    def compute_release(self, dataset, analysis, release):

        byte_buffer = lib_runtime.release(
            *_serialize_proto(dataset, ffi_runtime),
            *_serialize_proto(analysis, ffi_runtime),
            *_serialize_proto(release, ffi_runtime)
        )
        serialized_response = ffi_runtime.string(byte_buffer.data, byte_buffer.len)
        # lib_runtime.dp_runtime_destroy_bytebuffer(ctypes.pointer(byte_buffer))

        return release_pb2.Release.FromString(serialized_response)
