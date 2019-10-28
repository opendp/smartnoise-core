import subprocess
import queue
import os
import numpy as np

# protoc must be installed and on path
subprocess.call(f"protoc --python_out={os.getcwd()} *.proto", shell=True, cwd=os.path.abspath('../prototypes/'))

from burdock.wrapper import LibraryWrapper

# these modules are generated via the subprocess call
import analysis_pb2
import types_pb2
import release_pb2
import dataset_pb2


runtime_name = 'RUST'
validator_name = 'C++'

core_wrapper = LibraryWrapper(validator=validator_name, runtime=runtime_name)


class Dataset(object):
    def __init__(self, name, data):
        self.name = name
        self.data = data

        global context
        if context:
            context.datasets.append(self)

    def __getitem__(self, identifier):
        (column_id, datatype) = identifier
        typemap = {
            bytes: "BYTES",
            bool: "BOOL",
            int: "I64",
            float: "F64",
            str: "STRING",
        }
        if datatype in typemap:
            datatype = typemap[datatype]

        if datatype not in typemap.values():
            raise ValueError(f"Invalid datatype {datatype}. Datatype must be one of {list(typemap.values())}.")

        return Component('DataSource', options={
            'dataset_id': self.name,
            'column_id': column_id
        }, arguments={
            'datatype': Component('Literal', options={
                'value': array_nd(datatype)
            })
        })


class Component(object):
    def __init__(self, name: str, arguments: dict = None, options: dict = None):
        self.name: str = name
        self.arguments: dict = arguments or {}
        self.options: dict = options

        global context
        if context:
            context.components.append(self)

    def __add__(self, other):
        if type(other) != self.__class__:
            other = Component('Literal', options={'value': array_nd(other)})
        return Component('Add', {'left': self, 'right': other})

    def __sub__(self, other):
        if type(other) != self.__class__:
            other = Component('Literal', options={'value': array_nd(other)})
        return Component('Subtract', {'left': self, 'right': other})

    def __pos__(self):
        return self

    def __neg__(self):
        return Component('Negative', arguments={'data': self})

    def __mul__(self, other):
        if type(other) != self.__class__:
            other = Component('Literal', options={'value': array_nd(other)})
        return Component('Multiply', arguments={'left': self, 'right': other})

    def __truediv__(self, other):
        if type(other) != self.__class__:
            other = Component('Literal', options={'value': array_nd(other)})
        return Component('Divide', arguments={'left': self, 'right': other})

    def __pow__(self, power, modulo=None):
        if type(power) != self.__class__:
            power = Component('Literal', options={'value': array_nd(power)})
        return Component('Power', arguments={'left': self, 'right': power})


def mean(data):
    return Component('Mean', {'data': data})


def array_nd(data):

    if type(data) is bytes:
        return types_pb2.ArrayND(
            datatype=types_pb2.DataType.Value("BYTES"),
            bytes=data
        )

    data = np.array(data)

    data_type = {
        np.bool: "BOOL",
        np.int64: "I64",
        np.float64: "F64",
        np.string_: "STRING",
        np.str_: "STRING"
    }[data.dtype.type]

    container_type = {
        np.bool: types_pb2.Array1Dbool,
        np.int64: types_pb2.Array1Di64,
        np.float64: types_pb2.Array1Df64,
        np.string_: types_pb2.Array1Dstr,
        np.str_: types_pb2.Array1Dstr
    }[data.dtype.type]

    proto_args = {
        "datatype": data_type,
        data_type.lower(): container_type(data=list(data.flatten())),
        "shape": list(data.shape),
        "order": list(range(data.ndim))
    }

    return types_pb2.ArrayND(**proto_args)


def _to_component(value):
    return value if type(value) == Component else Component(
        'Literal', options={'value': array_nd(value)})


def dp_mean(data, epsilon, minimum, maximum, num_records):
    return Component('DPMean', {
        'data': _to_component(data),
        'num_records': _to_component(num_records),
        'minimum': _to_component(minimum),
        'maximum': _to_component(maximum)
    }, {
         'epsilon': epsilon,
         'mechanism': types_pb2.Mechanism.Value("LAPLACE")
     })


def dp_variance(data, epsilon, minimum, maximum, num_records):
    return Component('DPVariance', {
        'data': _to_component(data),
        'num_records': _to_component(num_records),
        'minimum': _to_component(minimum),
        'maximum': _to_component(maximum)
    }, {
         'epsilon': epsilon,
         'mechanism': types_pb2.Mechanism.Value("LAPLACE")
     })


def dp_covariance(data_x, data_y, epsilon, num_records, minimum_x, maximum_x, minimum_y, maximum_y):
    return Component('DPCovariance', {
        'data_x': _to_component(data_x),
        'data_y': _to_component(data_y),
        'num_records': _to_component(num_records),
        'minimum_x': _to_component(minimum_x),
        'maximum_x': _to_component(maximum_x),
        'minimum_y': _to_component(minimum_y),
        'maximum_y': _to_component(maximum_y)
    }, {
        'epsilon': epsilon,
        'mechanism': types_pb2.Mechanism.Value("LAPLACE")
    })


def dp_moment_raw(data, epsilon, minimum, maximum, num_records, order):
    return Component('DPMomentRaw', {
        'data': _to_component(data),
        'num_records': _to_component(num_records),
        'minimum': _to_component(minimum),
        'maximum': _to_component(maximum)
    }, {
        'epsilon': epsilon,
        'mechanism': types_pb2.Mechanism.Value("LAPLACE"),
        'order': order
     })


class Analysis(object):
    def __init__(self, *components, datasets=None, distance='APPROXIMATE', neighboring='SUBSTITUTE'):
        self.components: list = list(components)
        self.datasets: list = datasets or []
        self.release_proto: release_pb2.Release = None
        self.distance: str = distance
        self.neighboring: str = neighboring

        self._context_cache = None

    def _make_analysis_proto(self):

        id_count = 0

        discovered_components = {}
        component_queue = queue.Queue()

        def enqueue(component):
            if component in discovered_components:
                return discovered_components[component]

            nonlocal id_count
            id_count += 1

            discovered_components[component] = id_count
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
            privacy_definition=analysis_pb2.PrivacyDefinition(
                distance=analysis_pb2.PrivacyDefinition.Distance.Value(self.distance),
                neighboring=analysis_pb2.PrivacyDefinition.Neighboring.Value(self.neighboring)
            ),
            runtime_definition=analysis_pb2.RuntimeDefinition(
                name=runtime_name,
                version='0.1'
            )
        )

    def _make_release_proto(self):
        return self.release_proto or release_pb2.Release()

    def _make_dataset_proto(self):
        return dataset_pb2.Dataset(
            tables={
                dataset.name: dataset_pb2.Table(
                    file_path=dataset.data
                ) for dataset in self.datasets
            })

    def validate(self):
        return core_wrapper.validate_analysis(
            self._make_analysis_proto())

    @property
    def epsilon(self):
        return core_wrapper.compute_epsilon(
            self._make_analysis_proto())

    def release(self):
        analysis_proto: analysis_pb2.Analysis = self._make_analysis_proto()
        self.release_proto: release_pb2.Release = core_wrapper.compute_release(
            self._make_dataset_proto(),
            analysis_proto,
            self._make_release_proto())

        return core_wrapper.generate_report(
            analysis_proto,
            self.release_proto)

    def __enter__(self):
        global context
        self._context = context
        context = self
        return context

    def __exit__(self, exc_type, exc_val, exc_tb):
        global context
        context = self._context

    def _make_networkx(self):
        import networkx as nx

        analysis = self._make_analysis_proto()
        graph = nx.DiGraph()

        def label(node_id):
            return f'{node_id} {analysis.graph[node_id].WhichOneof("value")}'

        for nodeId, component in list(analysis.graph.items()):
            for field in component.arguments.values():
                graph.add_edge(label(field.source_node_id), label(nodeId))

        return graph

    def __str__(self):
        graph = self._make_networkx()
        out = "Analysis:\n"
        out += "\tGraph:\n"

        for sink in (node for node in graph if node.out_degree == 0):
            print(sink)

        return out

    def plot(self):
        import networkx as nx
        import matplotlib.pyplot as plt
        import warnings
        warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")

        graph = self._make_networkx()
        nx.draw(graph, with_labels=True, node_color='white')
        plt.pause(.001)


# sugary syntax for managing analysis contexts
context = None
