import json
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
    def __init__(self, *, path=None, value=None, private=True):

        global context
        if not context:
            raise ValueError("all Yarrow components must be created within the context of an analysis")

        if sum(int(i is not None) for i in [path, value]) != 1:
            raise ValueError("either path or value must be set")

        materialize_options = {'private': private}
        if path is not None:
            materialize_options['file_path'] = path
        if value is not None:
            materialize_options['literal'] = Component._make_value_proto(value)

        self.component = Component('Materialize', options=materialize_options)

    def __getitem__(self, identifier):
        return Component('Index', arguments={'columns': Component.of(identifier), 'data': self.component})


class Component(object):
    def __init__(self, name: str, arguments: dict = None, options: dict = None, constraints: dict = None, release=None):

        self.name: str = name
        self.arguments: dict = Component._expand_constraints(arguments or {}, constraints)
        self.options: dict = options

        # these are set when add_component is called
        self.analysis = None
        self.component_id = None

        global context
        if context:
            context.add_component(self, release=release)
        else:
            raise ValueError("all Yarrow components must be created within the context of an analysis")

    # pull the released values out from the analysis' release protobuf
    def __call__(self):
        return self.analysis.release_proto.get(self.component_id)

    def get(self):
        return self()

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
    # TODO: handle jagged
    def of(value, jagged=False):
        if value is None:
            return

        if type(value) == Component:
            return value

        return Component('Constant', release=value)

    @staticmethod
    def _make_value(data, is_jagged=False):
        if issubclass(type(data), dict):
            return value_pb2.Value(
                hashmap_string={key: Component._make_value(data[key]) for key in data}
            )

        if is_jagged:
            return value_pb2.Value(array_2d_jagged=value_pb2.Array2dJagged(data=[
                value_pb2.Array2dJagged.Array1dOption(data=column) for column in data
            ]))

        data = np.array(data)

        data_type = {
            np.bool: "bool",
            np.int64: "i64",
            np.float64: "f64",
            np.string_: "string",
            np.str_: "string"
        }[data.dtype.type]

        container_type = {
            np.bool: value_pb2.Array1dBool,
            np.int64: value_pb2.Array1dI64,
            np.float64: value_pb2.Array1dF64,
            np.string_: value_pb2.Array1dStr,
            np.str_: value_pb2.Array1dStr
        }[data.dtype.type]

        return value_pb2.Value(
            array_nd=value_pb2.ArrayNd(
                shape=list(data.shape),
                order=list(range(data.ndim)),
                flattened=value_pb2.Array1d(**{
                    data_type: container_type(data=list(data.flatten()))
                })
            ))

    @staticmethod
    def _expand_constraints(arguments, constraints):

        if not constraints:
            return arguments

        for argument in arguments.keys():
            filtered = [i[len(argument) + 1:] for i in constraints.keys()
                           if i.startswith(argument)]
            filtered = [i for i in filtered
                           if i in ALL_CONSTRAINTS]

            if 'max' in filtered and 'min' in filtered:
                min_component = Component.of(constraints[argument + '_min'])
                max_component = Component.of(constraints[argument + '_max'])

                arguments[argument] = Component('Clamp', arguments={
                    "data": arguments[argument],
                    "min": min_component,
                    "max": max_component
                })
                arguments[argument] = Component('Impute', arguments={
                    "data": arguments[argument]
                })

            else:
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
                arguments[argument] = Component('Clamp', arguments={
                    "data": arguments[argument],
                    "categories": Component.of(constraints[argument + '_categories'])
                })

            if 'n' in filtered:
                arguments[argument] = Component('Resize', arguments={
                    "data": arguments[argument],
                    "n": Component.of(constraints[argument + '_n'])
                })

        return arguments


class Analysis(object):
    def __init__(self, *components, datasets=None, distance='APPROXIMATE', neighboring='SUBSTITUTE'):

        # privacy definition
        self.distance: str = distance
        self.neighboring: str = neighboring

        # core data structures
        self.components: dict = {}
        self.release_values = {}
        self.datasets: list = datasets or []

        # TODO: temporary. should be converted into self.release_values upon return from runtime
        self.release_proto = None

        # track node ids
        self.component_count = 0
        for component in components:
            self.add_component(component)

        # nested analyses
        self._context_cache = None

    def add_component(self, component, release=None):
        if component.analysis:
            raise ValueError("this component is already a part of another analysis")

        # component should be able to reference back to the analysis to get released values/ownership
        component.analysis = self
        component.component_id = self.component_count

        if release is not None:
            self.release_values[self.component_count] = release
        self.components[self.component_count] = component
        self.component_count += 1

    def _make_analysis_proto(self):

        vertices = {}
        for component_id in self.components:
            component = self.components[component_id]

            vertices[component_id] = components_pb2.Component(**{
                'arguments': {
                    name: component_child.component_id
                    for name, component_child in component.arguments.items()
                    if component_child is not None
                },
                component.name.lower():
                    getattr(components_pb2, component.name)(**(component.options or {}))
            })

        return base_pb2.Analysis(
            computation_graph=base_pb2.ComputationGraph(value=vertices),
            privacy_definition=base_pb2.PrivacyDefinition(
                distance=base_pb2.PrivacyDefinition.Distance.Value(self.distance),
                neighboring=base_pb2.PrivacyDefinition.Neighboring.Value(self.neighboring)
            )
        )

    def _make_release_proto(self):

        return base_pb2.Release(
            values={
                component_id: base_pb2.ReleaseNode(
                    value=self._make_value_proto(self.release_values[component_id]),
                    privacy_usage=None)
                for component_id in self.components
                if component_id in self.release_values
            })

    @staticmethod
    def _make_value_proto(data, is_jagged=False):

        if issubclass(type(data), dict):
            return value_pb2.Value(
                hashmap_string={key: Component._make_value_proto(data[key], is_jagged=is_jagged) for key in data}
            )

        if is_jagged:
            return value_pb2.Value(array_2d_jagged=value_pb2.Array2dJagged(data=[
                value_pb2.Array2dJagged.Array1dOption(data=column) for column in data
            ]))

        data = np.array(data)

        data_type = {
            np.bool: "bool",
            np.int64: "i64",
            np.float64: "f64",
            np.string_: "string",
            np.str_: "string"
        }[data.dtype.type]

        container_type = {
            np.bool: value_pb2.Array1dBool,
            np.int64: value_pb2.Array1dI64,
            np.float64: value_pb2.Array1dF64,
            np.string_: value_pb2.Array1dStr,
            np.str_: value_pb2.Array1dStr
        }[data.dtype.type]

        return value_pb2.Value(
            array_nd=value_pb2.ArrayNd(
                shape=list(data.shape),
                order=list(range(data.ndim)),
                flattened=value_pb2.Array1d(**{
                    data_type: container_type(data=list(data.flatten()))
                })
            ))

    def validate(self):
        return core_wrapper.validate_analysis(
            self._make_analysis_proto(),
            self._make_release_proto())

    @property
    def epsilon(self):
        return core_wrapper.compute_privacy_usage(
            self._make_analysis_proto(),
            self._make_release_proto())

    def release(self):
        # TODO: convert into python representation
        self.release_proto: base_pb2.Release = core_wrapper.compute_release(
            self._make_analysis_proto(),
            self._make_release_proto())

        print(self.release_proto)

        return json.loads(core_wrapper.generate_report(
            self._make_analysis_proto(),
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
            return f'{node_id} {analysis.computation_graph.value[node_id].WhichOneof("value")}'

        for nodeId, component in list(analysis.computation_graph.value.items()):
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
