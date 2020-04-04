import json
import numpy as np

from whitenoise.wrapper import LibraryWrapper

# these modules are generated via the subprocess call
from whitenoise import base_pb2
from whitenoise import components_pb2
from whitenoise import value_pb2
import os

core_wrapper = LibraryWrapper()

ALL_CONSTRAINTS = ["n", "min", "max", "categories"]

variant_message_map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'variant_message_map.json')

with open(variant_message_map_path, 'r') as variant_message_map_file:
    variant_message_map = json.load(variant_message_map_file)


def privacy_usage(epsilon=None, delta=0):
    # upgrade epsilon/delta to lists if they aren't already
    if epsilon is not None and not issubclass(type(epsilon), list):
        epsilon = [epsilon]

    if delta is not None and not issubclass(type(delta), list):
        delta = [delta]

    if epsilon is not None and delta is not None:
        return [
            value_pb2.PrivacyUsage(
                distance_approximate=value_pb2.PrivacyUsage.DistanceApproximate(
                    epsilon=val_epsilon,
                    delta=val_delta
                )
            )
            for val_epsilon, val_delta in zip(epsilon, delta)
        ]

    if epsilon is not None and delta is None:
        return [
            value_pb2.PrivacyUsage(
                distance_pure=value_pb2.PrivacyUsage.DistancePure(
                    epsilon=val_epsilon
                )
            )
            for val_epsilon in epsilon
        ]

    # otherwise, no privacy usage


class Dataset(object):
    def __init__(self, *, path=None, value=None,
                 num_columns=None, column_names=None,
                 value_format=None, skip_row=True, private=True):

        global context
        if not context:
            raise ValueError("all whitenoise components must be created within the context of an analysis")

        if sum(int(i is not None) for i in [path, value]) != 1:
            raise ValueError("either path or value must be set")

        if num_columns is None and column_names is None:
            raise ValueError("either num_columns or column_names must be set")

        self.dataset_id = context.dataset_count
        context.dataset_count += 1

        data_source = {}
        if path is not None:
            data_source['file_path'] = path
        if value is not None:
            data_source['literal'] = Analysis._serialize_value_proto(value, value_format)

        self.component = Component('Materialize',
                                   arguments={
                                       "column_names": Component.of(column_names),
                                       "num_columns": Component.of(num_columns),
                                   },
                                   options={
                                       "data_source": value_pb2.DataSource(**data_source),
                                       "private": private,
                                       "dataset_id": value_pb2.I64Null(option=self.dataset_id),
                                       "skip_row": skip_row
                                   })

    def __getitem__(self, identifier):
        return Component('Index', arguments={'columns': Component.of(identifier), 'data': self.component})


class Component(object):
    def __init__(self, name: str,
                 arguments: dict = None, options: dict = None,
                 constraints: dict = None,
                 value=None, value_format=None):

        self.name: str = name
        self.arguments: dict = Component._expand_constraints(arguments or {}, constraints)
        self.options: dict = options

        # these are set when add_component is called
        self.analysis = None
        self.component_id = None

        global context
        if context:
            context.add_component(self, value=value, value_format=value_format)
        else:
            raise ValueError("all whitenoise components must be created within the context of an analysis")

    # pull the released values out from the analysis' release protobuf
    @property
    def value(self):
        return self.analysis.release_values.get(self.component_id, {"value": None})["value"]

    @property
    def actual_privacy_usage(self):
        return self.analysis.release_values.get(self.component_id, {"privacy_usage": None})["privacy_usage"]

    def get_usages(self):
        parents = [component for component in self.analysis.components.values()
                   if id(self) in list(id(i) for i in component.arguments.values())]

        return {parent: next(k for k, v in parent.arguments.items()
                             if id(self) == id(v)) for parent in parents}

    def get_accuracy(self, alpha):
        self.analysis.properties = core_wrapper.get_properties(
            self.analysis._serialize_analysis_proto(),
            self.analysis._serialize_release_proto()).properties

        return core_wrapper.privacy_usage_to_accuracy(
            privacy_definition=self.analysis._serialize_privacy_definition(),
            component=self.analysis._serialize_component(self),
            properties={name: self.analysis.properties.get(arg.component_id) for name, arg in self.arguments.items() if arg},
            alpha=alpha)

    def from_accuracy(self, value, alpha):
        return core_wrapper.accuracy_to_privacy_usage(
            privacy_definition=self.analysis._serialize_privacy_definition(),
            component=self.analysis._serialize_component(self),
            properties={name: self.analysis.properties.get(arg.component_id) for name, arg in self.arguments.items() if arg},
            accuracy=base_pb2.Accuracy(
                value=value,
                alpha=alpha))

    @property
    def properties(self):
        # TODO: this doesn't have to be recomputed every time
        self.analysis.properties = core_wrapper.get_properties(
            self.analysis._serialize_analysis_proto(),
            self.analysis._serialize_release_proto()).properties

        # TODO: parse into something human readable. Serialization is not necessary
        return self.analysis.properties.get(self.component_id)

    def __pos__(self):
        return self

    def __neg__(self):
        return Component('Negative', arguments={'data': self})

    def __add__(self, other):
        return Component('Add', {'left': self, 'right': Component.of(other)})

    def __radd__(self, other):
        return Component('Add', {'left': Component.of(other), 'right': self})

    def __sub__(self, other):
        return Component('Subtract', {'left': self, 'right': Component.of(other)})

    def __rsub__(self, other):
        return Component('Subtract', {'left': Component.of(other), 'right': self})

    def __mul__(self, other):
        return Component('Multiply', arguments={'left': self, 'right': Component.of(other)})

    def __rmul__(self, other):
        return Component('Multiply', arguments={'left': Component.of(other), 'right': self})

    def __div__(self, other):
        return Component('Divide', arguments={'left': self, 'right': Component.of(other)})

    def __truediv__(self, other):
        return Component('Divide', arguments={
            'left': Component('Cast', arguments={'data': self}, options={"type": "float"}),
            'right': Component('Cast', arguments={'data': Component.of(other)}, options={"type": "float"})})

    def __rtruediv__(self, other):
        return Component('Divide', arguments={'left': Component.of(other), 'right': self})

    def __mod__(self, other):
        return Component('Modulo', arguments={'left': self, 'right': Component.of(other)})

    def __rmod__(self, other):
        return Component('Modulo', arguments={'left': Component.of(other), 'right': self})

    def __pow__(self, power, modulo=None):
        return Component('Power', arguments={'data': self, 'radical': Component.of(power)})

    def __rpow__(self, other):
        return Component('Power', arguments={'left': Component.of(other), 'right': self})

    def __or__(self, other):
        return Component('Or', arguments={'left': self, 'right': Component.of(other)})

    def __ror__(self, other):
        return Component('Or', arguments={'left': Component.of(other), 'right': self})

    def __and__(self, other):
        return Component('And', arguments={'left': self, 'right': Component.of(other)})

    def __rand__(self, other):
        return Component('And', arguments={'left': Component.of(other), 'right': self})

    def __invert__(self):
        return Component('Negate', arguments={'data': self})

    def __xor__(self, other):
        return (self | other) & ~(self & other)

    def __gt__(self, other):
        return Component('GreaterThan', arguments={'left': self, 'right': Component.of(other)})

    def __ge__(self, other):
        return Component('GreaterThan', arguments={'left': self, 'right': Component.of(other)}) \
               or Component('Equal', arguments={'left': self, 'right': Component.of(other)})

    def __lt__(self, other):
        return Component('LessThan', arguments={'left': self, 'right': Component.of(other)})

    def __le__(self, other):
        return Component('LessThan', arguments={'left': self, 'right': Component.of(other)}) \
               or Component('Equal', arguments={'left': self, 'right': Component.of(other)})

    def __eq__(self, other):
        return Component('Equal', arguments={'left': self, 'right': Component.of(other)})

    def __ne__(self, other):
        return ~(self == other)

    def __abs__(self):
        return Component('Abs', arguments={'data': self})

    def __getitem__(self, identifier):
        return Component('Index', arguments={'columns': Component.of(identifier), 'data': self})

    def __hash__(self):
        return id(self)

    def __str__(self, depth=0):
        if self.value is not None and depth != 0:
            return str(self.value).replace("\n", "")

        inner = []
        if self.arguments:
            inner.append(",\n".join([f'{("  " * (depth + 1))}{name}={value.__str__(depth + 1)}' for name, value in self.arguments.items() if value is not None]))
        if self.options:
            inner.append(",\n".join([f'{("  " * (depth + 1))}{name}={str(value).replace(chr(10), "")}' for name, value in self.options.items() if value is not None]))

        if self.name == "Literal":
            inner = "released value: " + str(self.value).replace("\n", "")
        elif inner:
            inner = f'\n{("," + chr(10)).join(inner)}\n{("  " * depth)}'
        else:
            inner = ""

        return f'{self.name}({inner})'

    def __repr__(self):
        return f'<{self.component_id}: {self.name} Component>'

    @staticmethod
    def of(value, value_format=None):
        if value is None:
            return

        # count can take the entire dataset as an argument
        if type(value) == Dataset:
            value = value.component

        if type(value) == Component:
            return value

        return Component('Literal', value=value, value_format=value_format)

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

                # TODO: imputation on ints is unnecessary
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
    def __init__(self, *components, dynamic=False, datasets=None, distance='APPROXIMATE', neighboring='SUBSTITUTE'):

        # if false, validate the analysis before running it (enforces static validation)
        self.dynamic = dynamic

        # privacy definition
        self.distance: str = distance
        self.neighboring: str = neighboring

        # core data structures
        self.components: dict = {}
        self.release_values = {}
        self.datasets: list = datasets or []

        # track node ids
        self.component_count = 0
        for component in components:
            self.add_component(component)

        # track the number of datasets in use
        self.dataset_count = 0

        # nested analyses
        self._context_cache = None

    def add_component(self, component, value=None, value_format=None):
        if component.analysis:
            raise ValueError("this component is already a part of another analysis")

        # component should be able to reference back to the analysis to get released values/ownership
        component.analysis = self
        component.component_id = self.component_count

        if value is not None:
            self.release_values[self.component_count] = {
                'value': value,
                'value_format': value_format
            }
        self.components[self.component_count] = component
        self.component_count += 1

    def _serialize_privacy_definition(self):
        return base_pb2.PrivacyDefinition(
            distance=base_pb2.PrivacyDefinition.Distance.Value(self.distance),
            neighboring=base_pb2.PrivacyDefinition.Neighboring.Value(self.neighboring)
        )

    def _serialize_component(self, component):
        return components_pb2.Component(**{
            'arguments': {
                name: component_child.component_id
                for name, component_child in component.arguments.items()
                if component_child is not None
            },
            variant_message_map[component.name]:
                getattr(components_pb2, component.name)(**(component.options or {}))
        })

    def _serialize_analysis_proto(self):

        vertices = {}
        for component_id in self.components:
            vertices[component_id] = self._serialize_component(self.components[component_id])

        return base_pb2.Analysis(
            computation_graph=base_pb2.ComputationGraph(value=vertices),
            privacy_definition=self._serialize_privacy_definition()
        )

    def _serialize_release_proto(self):

        return base_pb2.Release(
            values={
                component_id: base_pb2.ReleaseNode(
                    value=self._serialize_value_proto(
                        self.release_values[component_id]['value'],
                        self.release_values[component_id].get("value_format")),
                    privacy_usage=privacy_usage(
                        **self.release_values[component_id].get("privacy_usage", {})))
                for component_id in self.release_values
            })

    @staticmethod
    def _parse_release_proto(release):
        def parse_release_node(release_node):
            parsed = {
                "value": Analysis._parse_value_proto(release_node.value),
                "value_format": release_node.value.WhichOneof("data")
            }
            if release_node.privacy_usage:
                parsed['privacy_usage'] = release_node.privacy_usage
            return parsed
        return {
            node_id: parse_release_node(release_node) for node_id, release_node in release.values.items()
        }

    @staticmethod
    def _serialize_value_proto(value, value_format=None):

        def make_array1d(array):

            data_type = {
                np.bool: "bool",
                np.int64: "i64",
                np.float64: "f64",
                np.bool_: "bool",
                np.string_: "string",
                np.str_: "string"
            }[array.dtype.type]

            container_type = {
                np.bool: value_pb2.Array1dBool,
                np.int64: value_pb2.Array1dI64,
                np.float64: value_pb2.Array1dF64,
                np.bool_: value_pb2.Array1dBool,
                np.string_: value_pb2.Array1dStr,
                np.str_: value_pb2.Array1dStr
            }[array.dtype.type]

            return value_pb2.Array1d(**{
                data_type: container_type(data=list(array))
            })

        if value_format == 'hashmap' or issubclass(type(value), dict):
            return value_pb2.Value(
                hashmap_string={key: Analysis._serialize_value_proto(value[key]) for key in value}
            )

        if value_format == 'jagged':
            if not issubclass(type(value), list):
                value = [value]
            if not any(issubclass(type(elem), list) for elem in value):
                value = [value]
            value = [elem if issubclass(type(elem), list) else [elem] for elem in value]

            return value_pb2.Value(jagged=value_pb2.Array2dJagged(
                data=[value_pb2.Array1dOption(option=None if column is None else make_array1d(np.array(column))) for
                      column in value],
                data_type=value_pb2.DataType
                    .Value({
                               np.bool: "BOOL",
                               np.int64: "I64",
                               np.float64: "F64",
                               np.bool_: "BOOL",
                               np.string_: "STRING",
                               np.str_: "STRING"
                           }[np.array(value[0]).dtype.type])
            ))

        if value_format is not None and value_format != 'array':
            raise ValueError('format must be either "array", "jagged", "hashmap" or None')

        array = np.array(value)

        return value_pb2.Value(
            array=value_pb2.ArrayNd(
                shape=list(array.shape),
                order=list(range(array.ndim)),
                flattened=make_array1d(array.flatten())
            ))

    @staticmethod
    def _parse_value_proto(value):

        def parse_array1d(array):
            data_type = array.WhichOneof("data")
            if data_type:
                return list(getattr(array, data_type).data)

        def parse_array1d_option(array):
            if array.HasField("option"):
                return parse_array1d(array.option)

        if value.HasField("array"):
            data = parse_array1d(value.array.flattened)
            if data:
                if value.array.shape:
                    return np.array(data).reshape(value.array.shape)
                return data[0]

        if value.HasField("hashmap"):
            return {k: Analysis._parse_value_proto(v) for k, v in value.hashmap_string.data.items()}

        if value.HasField("jagged"):
            return [
                parse_array1d_option(column) for column in value.jagged.data
            ]

    def validate(self):
        return core_wrapper.validate_analysis(
            self._serialize_analysis_proto(),
            self._serialize_release_proto()).value

    @property
    def privacy_usage(self):
        return core_wrapper.compute_privacy_usage(
            self._serialize_analysis_proto(),
            self._serialize_release_proto())

    def release(self):
        if not self.dynamic:
            assert self.validate(), "cannot release, analysis is not valid"

        release_proto: base_pb2.Release = core_wrapper.compute_release(
            self._serialize_analysis_proto(),
            self._serialize_release_proto())

        self.release_values = Analysis._parse_release_proto(release_proto)

    def report(self):
        return json.loads(core_wrapper.generate_report(
            self._serialize_analysis_proto(),
            self._serialize_release_proto()))

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

        analysis = self._serialize_analysis_proto()
        graph = nx.DiGraph()

        def label(node_id):
            return f'{node_id} {analysis.computation_graph.value[node_id].WhichOneof("variant")}'

        for nodeId, component in list(analysis.computation_graph.value.items()):
            for source_node_id in component.arguments.values():
                graph.add_edge(label(source_node_id), label(nodeId))

        return graph

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
