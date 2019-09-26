# generated from protoc command in README.md
import analysis_pb2
import types_pb2

import ctypes

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
        distance=types_pb2.PrivacyDefinition.Definition.Value('RENYI'),
        neighboring=types_pb2.PrivacyDefinition.Neighboring.Value('ADD_REMOVE')
    )
)

serialized = analysis.SerializeToString()
# print(analysis_pb2.Component.FromString(serialized))

lib_dp = ctypes.cdll.LoadLibrary('../base/cmake-build-debug/lib/libdifferential_privacy.so')
lib_dp.validate_analysis.argtypes = (ctypes.c_char_p,)
lib_dp.validate_analysis.restype = ctypes.c_bool

print(lib_dp.validate_analysis(ctypes.c_char_p(serialized)))
