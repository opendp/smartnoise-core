import ctypes
from prototypes import analysis_pb2

ID_count = 0


def get_id():
    global ID_count
    ID_count += 1
    return ID_count


data = analysis_pb2.Component(
    datasource=analysis_pb2.DataSource(datasetID='PUMS'))

constant = analysis_pb2.Component(
    constant=analysis_pb2.Constant(
        ID=get_id()))

data = analysis_pb2.Component(
    transformation=analysis_pb2.Transformation(
        ID=get_id(), name="add",
        arguments={"left": constant, "right": data}))

data = analysis_pb2.Component(
    mean=analysis_pb2.Mean(
        ID=get_id(),
        columnID="income",
        data=data))

serialized = data.SerializeToString()
# print(analysis_pb2.Component.FromString(serialized))

lib_dp = ctypes.cdll.LoadLibrary('../base/cmake-build-debug/lib/libdifferential_privacy.so')
lib_dp.validate.argtypes = (ctypes.c_char_p,)
lib_dp.validate.restype = ctypes.c_bool

print(lib_dp.validate(ctypes.c_char_p(serialized)))
