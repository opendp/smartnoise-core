import os
import json
import ctypes

from numpy.ctypeslib import ndpointer
import numpy as np

from sys import platform

import release_pb2

# turn on backtraces in rust
os.environ['RUST_BACKTRACE'] = 'full'  # '1'


prefix = {'win32': ''}.get(platform, 'lib')
extension = {'darwin': '.dylib', 'win32': '.dll'}.get(platform, '.so')

validator_paths = {
    "C++": f'../validator-c++/cmake-build-debug/lib/{prefix}differential_privacy{extension}',
    "HASKELL": f"../validator-haskell/.stack-work/install/x86_64-linux/" \
               f"148d0e92cd3f02b3b71e5e570acc02f4fd5aeac7a29166dac7a6b62c52d8796b/" \
               f"8.6.5/lib/{prefix}Validator{extension}"
}

runtime_paths = {
    "C++": f'../runtime-eigen/cmake-build-debug/lib/{prefix}differential_privacy_runtime_eigen{extension}',
    "RUST": f'../runtime-rust/target/release/{prefix}differential_privacy_runtime_rust{extension}'
}

protobuf_c_path = f'../validator-c++/cmake-build-debug/lib/{prefix}differential_privacy_proto{extension}'


class ByteBuffer(ctypes.Structure):
    _fields_ = [
        ("len", ctypes.c_uint64),
        ("data", ctypes.POINTER(ctypes.c_uint8))
    ]


def _serialize_proto(proto):
    serialized = proto.SerializeToString()
    # if type(proto) == analysis_pb2.Analysis:
    #     print(analysis_pb2.Analysis.FromString(serialized))
    bytes_array = bytearray(serialized)
    buffer = (ctypes.c_char * len(serialized)).from_buffer(bytes_array)
    return buffer, len(bytes_array)


class LibraryWrapper(object):
    def __init__(self, validator, runtime):

        self.validator = validator
        self.runtime = runtime

        validator_path = validator_paths[validator]
        runtime_path = runtime_paths[runtime]

        self.lib_dp_proto = ctypes.cdll.LoadLibrary(protobuf_c_path)

        # load validator functions
        self.lib_dp = ctypes.cdll.LoadLibrary(validator_path)
        self.lib_dp.validate_analysis.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
        self.lib_dp.validate_analysis.restype = ctypes.c_bool

        self.lib_dp.compute_epsilon.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
        self.lib_dp.compute_epsilon.restype = ctypes.c_double

        self.lib_dp.generate_report.argtypes = (
            ctypes.c_char_p, ctypes.c_int64,  # input analysis
            ctypes.c_char_p, ctypes.c_int64)  # input release
        self.lib_dp.generate_report.restype = ctypes.c_void_p

        self.lib_dp.free_ptr.argtypes = (ctypes.c_void_p,)

        if validator == "HASKELL":
            self.lib_dp.DPValidatorInit()

        # load runtime functions
        self.lib_runtime = ctypes.cdll.LoadLibrary(runtime_path)
        self.lib_runtime.release.argtypes = (
            ctypes.c_char_p, ctypes.c_int,  # input dataset
            ctypes.c_char_p, ctypes.c_int,  # input analysis
            ctypes.c_char_p, ctypes.c_int)  # input release
        self.lib_runtime.release.restype = ByteBuffer if runtime == "RUST" else ctypes.c_char_p

        # self.lib_runtime.dp_runtime_destroy_bytebuffer.argtypes = (ctypes.POINTER(ByteBuffer),)

        # _doublepp = ndpointer(dtype=np.uintp, ndim=1, flags='C')
        # self.lib_runtime.release_array.argtypes = (
        #     ctypes.c_int8, ctypes.c_int,  # input analysis
        #     ctypes.c_int8, ctypes.c_int,  # input release
        #     ctypes.c_int, ctypes.c_int, _doublepp)  # input data
        # self.lib_runtime.release_array.restype = ByteBuffer if runtime == "RUST" else ctypes.c_char_p

        self.lib_runtime.free_ptr.argtypes = (ctypes.c_void_p,)

    def __del__(self):
        if self.validator == "HASKELL":
            self.lib_dp.DPValidatorExit()

    def compute_epsilon(self, analysis):
        return self.lib_dp.compute_epsilon(
            *_serialize_proto(analysis)
        )

    def validate_analysis(self, analysis):
        return self.lib_dp.validate_analysis(
            *_serialize_proto(analysis)
        )

    def generate_report(self, analysis, release):
        serialized_report_ptr = self.lib_dp.generate_report(
            *_serialize_proto(analysis),
            *_serialize_proto(release)
        )

        serialized_report = ctypes.cast(serialized_report_ptr, ctypes.c_char_p).value
        self.lib_dp.free_ptr(ctypes.c_char_p(serialized_report_ptr))
        return json.loads(serialized_report)

    def compute_release(self, dataset, analysis, release):

        byte_buffer = self.lib_runtime.release(
            *_serialize_proto(dataset),
            *_serialize_proto(analysis),
            *_serialize_proto(release)
        )

        if self.runtime == 'RUST':
            serialized_response = ctypes.string_at(byte_buffer.data, byte_buffer.len)
        else:
            serialized_response = byte_buffer
        # self.lib_runtime.dp_runtime_destroy_bytebuffer(ctypes.pointer(byte_buffer))

        return release_pb2.Release.FromString(serialized_response)
