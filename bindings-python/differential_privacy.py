# generated from protoc command in README.md
import analysis_pb2
import types_pb2

import ctypes
import numpy as np
from numpy.ctypeslib import ndpointer

ID_count = 0


def get_id():
    global ID_count
    ID_count += 1
    return ID_count


data = analysis_pb2.Component(
    datasource=analysis_pb2.DataSource(datasetID='PUMS'))
data_id = get_id()

constant = analysis_pb2.Component(
    constant=analysis_pb2.Constant(
        name="test"
    ))
constant_id = get_id()

transform = analysis_pb2.Component(
    transformation=analysis_pb2.Transformation(
        name="add",
        arguments={"left": data_id, "right": constant_id}))
transform_id = get_id()

mean = analysis_pb2.Component(
    mean=analysis_pb2.Mean(
        argument=transform_id,
        columnID="income"))
mean_id = get_id()

analysis = analysis_pb2.Analysis(
    graph={
        data_id: data,
        constant_id: constant,
        transform_id: transform,
        mean_id: mean
    },
    definition=types_pb2.PrivacyDefinition(
        distance=types_pb2.PrivacyDefinition.Distance.Value('RENYI'),
        neighboring=types_pb2.PrivacyDefinition.Neighboring.Value('ADD_REMOVE')
    )
)

serialized = analysis.SerializeToString()
# print(analysis_pb2.Component.FromString(serialized))

lib_dp = ctypes.cdll.LoadLibrary('../base/cmake-build-debug/lib/libdifferential_privacy.so')
lib_dp.validate_analysis.argtypes = (ctypes.c_char_p,)
lib_dp.validate_analysis.restype = ctypes.c_bool

print(lib_dp.validate_analysis(ctypes.c_char_p(serialized)))


def release(analysis, data):
    _doublepp = ndpointer(dtype=np.uintp, ndim=1, flags='C')

    lib_runtime = ctypes.cdll.LoadLibrary(
        '../runtime-eigen/cmake-build-debug/lib/libdifferential_privacy_runtime_eigen.so')
    lib_runtime.release.argtypes = (ctypes.c_char_p, ctypes.c_int, ctypes.c_int, _doublepp)
    lib_runtime.release.restype = ctypes.c_char_p

    response = lib_runtime.release(
        ctypes.c_char_p(analysis.SerializeToSring()),
        ctypes.c_int(data.shape[0]),
        ctypes.c_int(data.shape[1]),
        (data.__array_interface__['data'][0] + np.arange(data.shape[0]) * data.strides[0]).astype(np.uintp)
    )

    print(type(response))
    print(analysis_pb2.Component.FromString(response))


release(analysis, np.array([[1], [5], [3]]))
