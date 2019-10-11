import ctypes
import numpy as np
from numpy.ctypeslib import ndpointer
import queue
import json
import pandas

# protobuf is generated from the command in the readme
import analysis_pb2
import types_pb2
import release_pb2
from sys import platform

extension = None
if platform == "linux" or platform == "linux2":
    extension = ".so"
elif platform == "darwin":
    extension = ".dylib"
elif platform == "win32":
    extension = ".dll"

runtime_library = 'HASKELL'

validator_path = None
if runtime_library == 'C++':
    validator_path = '../validator-c++/cmake-build-debug/lib/libdifferential_privacy' + extension
if runtime_library == 'HASKELL':
    validator_path = '../validator-fortran/libdifferential_privacy' + extension

protobuf_c_path = '../validator-c++/cmake-build-debug/lib/libdifferential_privacy_proto' + extension
runtime_path = '../runtime-eigen/cmake-build-debug/lib/libdifferential_privacy_runtime_eigen' + extension

lib_dp = ctypes.cdll.LoadLibrary(protobuf_c_path)
# load validator functions
lib_dp = ctypes.cdll.LoadLibrary(validator_path)
lib_dp.validateAnalysis.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
lib_dp.validateAnalysis.restype = ctypes.c_bool

lib_dp.computeEpsilon.argtypes = (ctypes.c_char_p, ctypes.c_int64)  # input analysis
lib_dp.computeEpsilon.restype = ctypes.c_double

lib_dp.generateReport.argtypes = (
    ctypes.c_char_p, ctypes.c_int64,  # input analysis
    ctypes.c_char_p, ctypes.c_int64)  # input release
lib_dp.generateReport.restype = ctypes.c_char_p

# load runtime functions
lib_runtime = ctypes.cdll.LoadLibrary(runtime_path)
lib_runtime.release.argtypes = (
    ctypes.c_char_p, ctypes.c_int,  # input analysis
    ctypes.c_char_p, ctypes.c_int,  # input release
    ctypes.c_char_p, ctypes.c_int,  # input data path
    ctypes.c_char_p, ctypes.c_int)  # input columns
lib_runtime.release.restype = ctypes.c_char_p

_doublepp = ndpointer(dtype=np.uintp, ndim=1, flags='C')
lib_runtime.releaseArray.argtypes = (
    ctypes.c_char_p, ctypes.c_int,  # input analysis
    ctypes.c_char_p, ctypes.c_int,  # input release
    ctypes.c_int, ctypes.c_int, _doublepp,  # input data
    ctypes.c_char_p, ctypes.c_int)  # input columns
lib_runtime.releaseArray.restype = ctypes.c_char_p


class Component(object):
    def __init__(self, name: str, arguments: dict = None, options: dict = None):
        self.name: str = name
        self.arguments: dict = arguments or {}
        self.options: dict = options

        global context
        if context:
            context.components.append(self)

    def __add__(self, other):
        return Component('Add', {'left': self, 'right': other})

    def __sub__(self, other):
        return Component('Add', {'left': self, 'right': -other})

    def __neg__(self):
        return Component('Negate', {'x': self})


def mean(x):
    return Component('Mean', {'x': x})


class Analysis(object):
    def __init__(self, *components, distance='APPROXIMATE', neighboring='SUBSTITUTE'):
        self.components: list = list(components)
        self.release: release_pb2.Release = None
        self.distance: str = distance
        self.neighboring: str = neighboring

        self._context_cache = None

    def _make_analysis_proto(self):

        id_count = 0

        discovered_components = set()
        component_queue = queue.Queue()

        def enqueue(component):
            if component in discovered_components:
                return
            discovered_components.add(component)

            nonlocal id_count
            id_count += 1

            component_queue.put({'component_id': id_count, 'component': component})
            return id_count

        for component in self.components:
            enqueue(component)

        vertices = {}

        while not component_queue.empty():
            item = component_queue.get()
            component = item['component']
            component_id = item['component_id']

            vertices[component_id] = analysis_pb2.Component(**{
                'arguments': {
                    name: analysis_pb2.Component.Field(
                        source_node_id=enqueue(component_child),
                        # TODO: this is not always necessarily data! if a component has multiple outputs...
                        source_field="data"
                    ) for name, component_child in component.arguments.items()
                },
                component.name.lower():
                    getattr(analysis_pb2, component.name)(**(component.options or {}))
            })

        return analysis_pb2.Analysis(
            graph=vertices,
            definition=types_pb2.PrivacyDefinition(
                distance=types_pb2.PrivacyDefinition.Distance.Value(self.distance),
                neighboring=types_pb2.PrivacyDefinition.Neighboring.Value(self.neighboring)
            )
        )

    def _make_release_proto(self):
        return self.release or release_pb2.Release()

    def validate(self):
        serialized_analysis = self._make_analysis_proto().SerializeToString()
        # print(analysis_pb2.Analysis.FromString(serialized))
        return lib_dp.validateAnalysis(ctypes.c_char_p(serialized_analysis), len(serialized_analysis))

    @property
    def epsilon(self):
        serialized_analysis = self._make_analysis_proto().SerializeToString()
        return lib_dp.computeEpsilon(ctypes.c_char_p(serialized_analysis), len(serialized_analysis))

    def release(self, data):
        serialized_analysis = self._make_analysis_proto().SerializeToString()
        serialized_release = self._make_release_proto().SerializeToString()

        if type(data) == str:
            with open(data, 'r') as datafile:
                header = datafile.readline()

            serialized_response = lib_runtime.release(
                ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
                ctypes.c_char_p(serialized_release), len(serialized_release),
                ctypes.c_char_p(data), len(data),
                ctypes.c_char_p(header), len(header)
            )
            self.release = release_pb2.Release.FromString(serialized_response)

        if type(data) == pandas.DataFrame:

            array = data.to_numpy()
            header = '.'.join(data.columns.values)

            if len(data.shape) != 2:
                raise ValueError('data must be a 2-dimensional array')

            serialized_response = lib_runtime.release(
                ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
                ctypes.c_char_p(serialized_release), len(serialized_release),
                *[ctypes.c_int(i) for i in array.shape],
                (array.__array_interface__['data'][0] + np.arange(array.shape[0]) * array.strides[0]).astype(np.uintp),
                ctypes.c_char_p(header), len(header)
            )

            self.release = analysis_pb2.Component.FromString(serialized_response)

        print(self.release)

        serialized_release = self._make_release_proto().SerializeToString()
        serialized_report = lib_runtime.generateReport(
            ctypes.c_char_p(serialized_analysis), len(serialized_analysis),
            ctypes.c_char_p(serialized_release), len(serialized_release)
        )

        return json.loads(serialized_report)

    def __enter__(self):
        global context
        self._context = context
        context = self
        return context

    def __exit__(self, exc_type, exc_val, exc_tb):
        global context
        context = self._context


# sugary syntax for managing analysis contexts
context = None
