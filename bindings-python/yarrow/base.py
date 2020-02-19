import json
import queue
import numpy as np

from yarrow.wrapper import LibraryWrapper

# these modules are generated via the subprocess call
from yarrow import base_pb2
from yarrow import components_pb2
from yarrow import value_pb2

core_wrapper = LibraryWrapper()

ALL_CONSTRAINTS = ["n", "min", "max", "categories"]


def privacy_usage(epsilon=None, delta=None):
    if epsilon is not None and delta is not None:
        return value_pb2.PrivacyUsage(
            distance_approximate=value_pb2.PrivacyUsage.DistanceApproximate(
                epsilon=epsilon,
                delta=delta
            )
        )

    if epsilon is not None and delta is None:
        return value_pb2.PrivacyUsage(
            distance_pure=value_pb2.PrivacyUsage.DistancePure(
                epsilon=epsilon
            )
        )

    raise ValueError("Unknown privacy definition.")


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
            'datatype': Component.of(datatype)
        })


class Component(object):
    def __init__(self, name: str, arguments: dict = None, options: dict = None, constraints: dict = None):

        self.name: str = name
        self.arguments: dict = Component._expand_constraints(arguments or {}, constraints)
        self.options: dict = options

        global context
        if context:
            context.components.append(self)

    def __pos__(self):
        return self

    def __neg__(self):
        return Component('Negative', arguments={'data': self})

    def __add__(self, other):
        return Component('Add', {'left': self, 'right': Component.of(other)})

    def __sub__(self, other):
        return Component('Subtract', {'left': self, 'right': Component.of(other)})

    def __mul__(self, other):
        return Component('Multiply', arguments={'left': self, 'right': Component.of(other)})

    def __truediv__(self, other):
        return Component('Divide', arguments={'left': self, 'right': Component.of(other)})

    def __pow__(self, power, modulo=None):
        return Component('Power', arguments={'left': self, 'right': Component.of(power)})

    def __or__(self, other):
        return Component('Or', arguments={'left': self, 'right': Component.of(other)})

    def __and__(self, other):
        return Component('And', arguments={'left': self, 'right': Component.of(other)})

    def __gt__(self, other):
        return Component('GreaterThan', arguments={'left': self, 'right': Component.of(other)})

    def __lt__(self, other):
        return Component('LessThan', arguments={'left': self, 'right': Component.of(other)})

    def __eq__(self, other):
        return Component('Equal', arguments={'left': self, 'right': Component.of(other)})

    def __gte__(self, other):
        other = Component.of(other)
        return Component('GreaterThan', arguments={'left': self, 'right': other}) or \
               Component('Equal', arguments={'left': self, 'right': other})

    def __lte__(self, other):
        other = Component.of(other)
        return Component('LessThan', arguments={'left': self, 'right': other}) or \
               Component('Equal', arguments={'left': self, 'right': other})

    def __hash__(self):
        return id(self)

    @staticmethod
    def of(value):
        def value_proto(data):

            if type(data) is bytes:
                return value_pb2.Value(
                    datatype=value_pb2.DataType.Value("BYTES"),
                    bytes=data
                )

            if issubclass(type(data), dict):
                return value_pb2.Value(
                    datatype=value_pb2.DataType.Value("HASHMAP_STRING"),
                    hashmapString={key: value_proto(data[key]) for key in data}
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
                np.bool: value_pb2.Array1Dbool,
                np.int64: value_pb2.Array1Di64,
                np.float64: value_pb2.Array1Df64,
                np.string_: value_pb2.Array1Dstr,
                np.str_: value_pb2.Array1Dstr
            }[data.dtype.type]

            proto_args = {
                "datatype": data_type,
                data_type.lower(): container_type(
                    data=list(data.flatten()),
                    shape=list(data.shape),
                    order=list(range(data.ndim)))
            }

            return value_pb2.Value(**proto_args)

        return value if type(value) == Component else Component(
            'Literal', options={'value': value_proto(value)})

    @staticmethod
    def _expand_constraints(arguments, constraints):

        if not constraints:
            return arguments

        for argument in arguments.keys():
            filtered = [i[len(argument) + 1:] for i in constraints.keys()
                           if i.startswith(argument)]
            filtered = [i for i in filtered
                           if i in ALL_CONSTRAINTS]

            if 'max' in filtered:
                arguments[argument] = Component('RowMax', arguments={
                    "left": arguments[argument],
                    "right": Component.of(constraints[argument + '_max'])
                })

            if 'min' in filtered:
                arguments[argument] = Component('RowMin', arguments={
                    "left": arguments[argument],
                    "right": Component.of(constraints[argument + '_min'])
                })

            if 'categories' in filtered:
                arguments[argument] = Component('Bin', arguments={
                    "data": arguments[argument],
                    "categories": Component.of(constraints[argument + '_categories'])
                })

            if 'n' in filtered:
                arguments[argument] = Component('Impute', arguments={
                    "data": arguments[argument],
                    "n": Component.of(constraints[argument + '_n'])
                })

        return arguments


class Analysis(object):
    def __init__(self, *components, datasets=None, distance='APPROXIMATE', neighboring='SUBSTITUTE'):
        self.components: list = list(components)
        self.datasets: list = datasets or []
        self.release_proto: base_pb2.Release = None
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

            vertices[component_id] = components_pb2.Component(**{
                'arguments': {
                    name: enqueue(component_child) for name, component_child in component.arguments.items()
                },
                component.name.lower():
                    getattr(components_pb2, component.name)(**(component.options or {}))
            })

        return base_pb2.Analysis(
            graph=vertices,
            privacy_definition=base_pb2.PrivacyDefinition(
                distance=base_pb2.PrivacyDefinition.Distance.Value(self.distance),
                neighboring=base_pb2.PrivacyDefinition.Neighboring.Value(self.neighboring)
            )
        )

    def _make_release_proto(self):
        return self.release_proto or base_pb2.Release()

    def _make_dataset_proto(self):
        return base_pb2.Dataset(
            tables={
                dataset.name: base_pb2.Table(
                    file_path=dataset.data
                ) for dataset in self.datasets
            })

    def validate(self):
        return core_wrapper.validate_analysis(
            self._make_analysis_proto())

    @property
    def epsilon(self):
        return core_wrapper.compute_privacy_usage(
            self._make_analysis_proto(),
            self._make_release_proto())

    def release(self):
        analysis_proto: base_pb2.Analysis = self._make_analysis_proto()
        self.release_proto: base_pb2.Release = core_wrapper.compute_release(
            self._make_dataset_proto(),
            analysis_proto,
            self._make_release_proto())

        return json.loads(core_wrapper.generate_report(
            analysis_proto,
            self.release_proto))

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
            for source_node_id in component.arguments.values():
                graph.add_edge(label(source_node_id), label(nodeId))

        return graph

    # def __str__(self):
    #     graph = self._make_networkx()
    #     out = "Analysis:\n"
    #     out += "\tGraph:\n"
    #
    #     for sink in (node for node in graph if node.out_degree == 0):
    #         print(sink)
    #
    #     return out

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
