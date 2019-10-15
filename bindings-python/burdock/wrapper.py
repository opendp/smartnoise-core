import json
import ctypes

import pandas
from numpy.ctypeslib import ndpointer
import numpy as np

from sys import platform

import release_pb2

extension = None
if platform == "linux" or platform == "linux2":
    extension = ".so"
elif platform == "darwin":
    extension = ".dylib"
elif platform == "win32":
    extension = ".dll"

validator_paths = {
    "C++": '../validator-c++/cmake-build-debug/lib/libdifferential_privacy' + extension,
    "HASKELL": '../validator-haskell/libdifferential_privacy' + extension
}

runtime_paths = {
    "C++": '../runtime-eigen/cmake-build-debug/lib/libdifferential_privacy_runtime_eigen' + extension,
    "RUST": '../runtime-rust/target/release/libdifferential_privacy_runtime_rust' + extension
}

protobuf_c_path = '../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto' + extension


class LibraryWrapper(object):
    def __init__(self, validator, runtime):

        validator_path = validator_paths[validator]
        runtime_path = runtime_paths[runtime]

        self.lib_dp_proto = ctypes.cdll.LoadLibrary(protobuf_c_path)

        # load validator functions
        self.lib_dp = ctypes.cdll.LoadLibrary(validator_path)
        self.lib_dp.validateAnalysis.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
        self.lib_dp.validateAnalysis.restype = ctypes.c_bool

        self.lib_dp.computeEpsilon.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
        self.lib_dp.computeEpsilon.restype = ctypes.c_double

        self.lib_dp.generateReport.argtypes = (
            ctypes.c_char_p, ctypes.c_int64,  # input analysis
            ctypes.c_char_p, ctypes.c_int64)  # input release
        self.lib_dp.generateReport.restype = ctypes.c_void_p

        self.lib_dp.freePtr.argtypes = (ctypes.c_void_p,)

        # load runtime functions
        self.lib_runtime = ctypes.cdll.LoadLibrary(runtime_path)
        self.lib_runtime.release.argtypes = (
            ctypes.c_char_p, ctypes.c_int,  # input analysis
            ctypes.c_char_p, ctypes.c_int,  # input release
            ctypes.c_char_p, ctypes.c_int,  # input data path
            ctypes.c_char_p, ctypes.c_int)  # input columns
        self.lib_runtime.release.restype = ctypes.c_char_p

        _doublepp = ndpointer(dtype=np.uintp, ndim=1, flags='C')
        self.lib_runtime.releaseArray.argtypes = (
            ctypes.c_char_p, ctypes.c_int,  # input analysis
            ctypes.c_char_p, ctypes.c_int,  # input release
            ctypes.c_int, ctypes.c_int, _doublepp,  # input data
            ctypes.c_char_p, ctypes.c_int)  # input columns
        self.lib_runtime.releaseArray.restype = ctypes.c_void_p

        self.lib_runtime.freePtr.argtypes = (ctypes.c_void_p,)

    def computeEpsilon(self, analysis):
        serialized = analysis.SerializeToString()
        # print(analysis_pb2.Analysis.FromString(serialized))
        return self.lib_dp.computeEpsilon(ctypes.c_char_p(serialized), len(serialized))

    def validateAnalysis(self, analysis):
        serialized = analysis.SerializeToString()
        return self.lib_dp.validateAnalysis(ctypes.c_char_p(serialized), len(serialized))

    def generateReport(self, analysis, release):
        serialized_analysis = analysis.SerializeToString()
        serialized_release = release.SerializeToString()

        serialized_report_ptr = self.lib_dp.generateReport(
            ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
            ctypes.c_char_p(serialized_release), len(serialized_release)
        )
        serialized_report = ctypes.cast(serialized_report_ptr, ctypes.c_char_p).value
        self.lib_dp.freePtr(ctypes.c_char_p(serialized_report_ptr))
        return json.loads(serialized_report)

    def computeRelease(self, analysis, release, data):
        serialized_analysis = analysis.SerializeToString()
        serialized_release = release.SerializeToString()

        if type(data) == str:
            with open(data, 'r') as datafile:
                header = datafile.readline()

            serialized_response_ptr = self.lib_runtime.release(
                ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
                ctypes.c_char_p(serialized_release), len(serialized_release),
                ctypes.c_char_p(data), len(data),
                ctypes.c_char_p(header), len(header)
            )
            serialized_response = ctypes.cast(serialized_response_ptr, ctypes.c_char_p).value
            self.lib_runtime.freePtr(serialized_response_ptr)

            return release_pb2.Release.FromString(serialized_response)

        if type(data) == pandas.DataFrame:

            array = data.to_numpy()
            header = '.'.join(data.columns.values).encode('utf-8')

            if len(data.shape) != 2:
                raise ValueError('data must be a 2-dimensional array')

            serialized_response_ptr = self.lib_runtime.releaseArray(
                ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
                ctypes.c_char_p(serialized_release), len(serialized_release),
                *[ctypes.c_int(i) for i in array.shape],
                (array.__array_interface__['data'][0] + np.arange(array.shape[0]) * array.strides[0]).astype(np.uintp),
                ctypes.c_char_p(header), len(header)
            )
            serialized_response = ctypes.cast(serialized_response_ptr, ctypes.c_char_p).value

            self.lib_runtime.freePtr(ctypes.c_char_p(serialized_response_ptr))

            return release_pb2.Release.FromString(serialized_response)
