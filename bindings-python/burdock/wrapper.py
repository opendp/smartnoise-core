import os
import json
import ctypes

import pandas
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
    "HASKELL": f'../validator-haskell/{prefix}differential_privacy{extension}'
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

        # load runtime functions
        self.lib_runtime = ctypes.cdll.LoadLibrary(runtime_path)
        self.lib_runtime.release.argtypes = (
            ctypes.c_char_p, ctypes.c_int,  # input analysis
            ctypes.c_char_p, ctypes.c_int,  # input release
            ctypes.c_char_p, ctypes.c_int)  # input data path
        self.lib_runtime.release.restype = ByteBuffer if runtime == "RUST" else ctypes.c_char_p

        # self.lib_runtime.dp_runtime_destroy_bytebuffer.argtypes = (ctypes.POINTER(ByteBuffer),)

        _doublepp = ndpointer(dtype=np.uintp, ndim=1, flags='C')
        self.lib_runtime.release_array.argtypes = (
            ctypes.c_int8, ctypes.c_int,  # input analysis
            ctypes.c_int8, ctypes.c_int,  # input release
            ctypes.c_int, ctypes.c_int, _doublepp)  # input data
        self.lib_runtime.release_array.restype = ByteBuffer if runtime == "RUST" else ctypes.c_char_p

        self.lib_runtime.free_ptr.argtypes = (ctypes.c_void_p,)

    def computeEpsilon(self, analysis):
        serialized_analysis = analysis.SerializeToString()
        # print(analysis_pb2.Analysis.FromString(serialized_analysis))

        char_array_analysis = ctypes.c_char * len(serialized_analysis)
        bytes_analysis = bytearray(serialized_analysis)
        buffer_analysis = char_array_analysis.from_buffer(bytes_analysis)

        return self.lib_dp.compute_epsilon(buffer_analysis, len(bytes_analysis))

    def validateAnalysis(self, analysis):
        serialized_analysis = analysis.SerializeToString()

        char_array_analysis = ctypes.c_char * len(serialized_analysis)
        bytes_analysis = bytearray(serialized_analysis)
        buffer_analysis = char_array_analysis.from_buffer(bytes_analysis)

        return self.lib_dp.validate_analysis(buffer_analysis, len(bytes_analysis))

    def generateReport(self, analysis, release):
        serialized_analysis = analysis.SerializeToString()
        serialized_release = release.SerializeToString()

        char_array_analysis = ctypes.c_char * len(serialized_analysis)
        bytes_analysis = bytearray(serialized_analysis)
        buffer_analysis = char_array_analysis.from_buffer(bytes_analysis)

        char_array_release = ctypes.c_char * len(serialized_release)
        bytes_release = bytearray(serialized_release)
        buffer_release = char_array_release.from_buffer(bytes_release)

        serialized_report_ptr = self.lib_dp.generate_report(
            buffer_analysis, len(bytes_analysis),
            buffer_release, len(bytes_release),
        )
        serialized_report = ctypes.cast(serialized_report_ptr, ctypes.c_char_p).value
        self.lib_dp.free_ptr(ctypes.c_char_p(serialized_report_ptr))
        return json.loads(serialized_report)

    def computeRelease(self, analysis, release, data):
        serialized_analysis = analysis.SerializeToString()
        serialized_release = release.SerializeToString()

        if type(data) == str:
            with open(data, 'r') as datafile:
                header = datafile.readline().encode('utf-8')

            char_array_analysis = ctypes.c_char * len(serialized_analysis)
            bytes_analysis = bytearray(serialized_analysis)
            buffer_analysis = char_array_analysis.from_buffer(bytes_analysis)

            char_array_release = ctypes.c_char * len(serialized_release)
            bytes_release = bytearray(serialized_release)
            buffer_release = char_array_release.from_buffer(bytes_release)

            byte_buffer = self.lib_runtime.release(
                buffer_analysis, len(bytes_analysis),
                buffer_release, len(bytes_release),
                ctypes.c_char_p(data.encode('utf-8')), len(data),
                ctypes.c_char_p(header), len(header)
            )

            if self.runtime == 'RUST':
                serialized_response = ctypes.string_at(byte_buffer.data, byte_buffer.len)
            else:
                serialized_response = byte_buffer
            # self.lib_runtime.dp_runtime_destroy_bytebuffer(ctypes.pointer(byte_buffer))

            return release_pb2.Release.FromString(serialized_response)

        if type(data) == pandas.DataFrame:

            array = data.to_numpy()
            header = '.'.join(data.columns.values).encode('utf-8')

            if len(data.shape) != 2:
                raise ValueError('data must be a 2-dimensional array')

            char_array_analysis = ctypes.c_char * len(serialized_analysis)
            bytes_analysis = bytearray(serialized_analysis)
            buffer_analysis = char_array_analysis.from_buffer(bytes_analysis)

            char_array_release = ctypes.c_char * len(serialized_release)
            bytes_release = bytearray(serialized_release)
            buffer_release = char_array_release.from_buffer(bytes_release)

            byte_buffer = self.lib_runtime.release_array(
                buffer_analysis, len(bytes_analysis),
                buffer_release, len(bytes_release),
                *[ctypes.c_int(i) for i in array.shape],
                (array.__array_interface__['data'][0] + np.arange(array.shape[0]) * array.strides[0]).astype(np.uintp),
                ctypes.c_char_p(header), len(header)
            )
            if self.runtime == 'RUST':
                serialized_response = ctypes.string_at(byte_buffer.data, byte_buffer.len)
            else:
                serialized_response = byte_buffer

            # self.lib_runtime.dp_runtime_destroy_bytebuffer(ctypes.pointer(byte_buffer))
            return release_pb2.Release.FromString(serialized_response)
