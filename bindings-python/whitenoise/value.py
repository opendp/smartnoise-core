from whitenoise import base_pb2
from whitenoise import components_pb2
from whitenoise import value_pb2
import whitenoise

import os
import json
import numpy as np


variant_message_map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'variant_message_map.json')

with open(variant_message_map_path, 'r') as variant_message_map_file:
    variant_message_map = json.load(variant_message_map_file)


def serialize_privacy_usage(epsilon=None, delta=0):
    """
    Construct a protobuf object representing privacy usage

    :param epsilon:
    :param delta:
    :return:
    """
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


def serialize_privacy_definition(analysis):
    return base_pb2.PrivacyDefinition(
        distance=base_pb2.PrivacyDefinition.Distance.Value(analysis.distance),
        neighboring=base_pb2.PrivacyDefinition.Neighboring.Value(analysis.neighboring)
    )


def serialize_component(component):
    return components_pb2.Component(**{
        'arguments': {
            name: component_child.component_id
            for name, component_child in component.arguments.items()
            if component_child is not None
        },
        variant_message_map[component.name]:
            getattr(components_pb2, component.name)(**(component.options or {}))
    })


def serialize_analysis_proto(analysis):
    vertices = {}
    for component_id in analysis.components:
        vertices[component_id] = serialize_component(analysis.components[component_id])

    return base_pb2.Analysis(
        computation_graph=base_pb2.ComputationGraph(value=vertices),
        privacy_definition=serialize_privacy_definition(analysis)
    )


def serialize_release_proto(release_values):
    return base_pb2.Release(
        values={
            component_id: base_pb2.ReleaseNode(
                value=serialize_value_proto(
                    release_values[component_id]['value'],
                    release_values[component_id].get("value_format")),
                privacy_usage=whitenoise.serialize_privacy_usage(
                    **release_values[component_id].get("privacy_usage", {})))
            for component_id in release_values
        })


def serialize_array1d(array):
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


def serialize_value_proto(value, value_format=None):

    if value_format == 'hashmap' or issubclass(type(value), dict):
        return value_pb2.Value(
            hashmap_string={key: serialize_value_proto(value[key]) for key in value}
        )

    if value_format == 'jagged':
        if not issubclass(type(value), list):
            value = [value]
        if not any(issubclass(type(elem), list) for elem in value):
            value = [value]
        value = [elem if issubclass(type(elem), list) else [elem] for elem in value]

        return value_pb2.Value(jagged=value_pb2.Array2dJagged(
            data=[value_pb2.Array1dOption(option=None if column is None else serialize_array1d(np.array(column))) for
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
            flattened=serialize_array1d(array.flatten())
        ))


def parse_array1d_null(array):
    data_type = array.WhichOneof("data")
    if not data_type:
        return

    return [v.option if v.HasField("option") else None for v in list(getattr(array, data_type).data)]


def parse_array1d(array):
    data_type = array.WhichOneof("data")
    if data_type:
        return list(getattr(array, data_type).data)


def parse_array1d_option(array):
    if array.HasField("option"):
        return parse_array1d(array.option)


def parse_jagged(value):
    return [
        parse_array1d_option(column) for column in value.jagged.data
    ]


def parse_array(value):
    data = parse_array1d(value.array.flattened)
    if data:
        if value.array.shape:
            return np.array(data).reshape(value.array.shape)
        return data[0]


def parse_hashmap(value):
    return {k: parse_value_proto(v) for k, v in value.hashmap_string.data.items()}


def parse_value_proto(value):
    if value.HasField("array"):
        return parse_array(value)

    if value.HasField("hashmap"):
        return parse_hashmap(value)

    if value.HasField("jagged"):
        return parse_jagged(value)


def parse_release_proto(release):

    def parse_release_node(release_node):
        parsed = {
            "value": parse_value_proto(release_node.value),
            "value_format": release_node.value.WhichOneof("data")
        }
        if release_node.privacy_usage:
            parsed['privacy_usage'] = release_node.privacy_usage
        return parsed
    return {
        node_id: parse_release_node(release_node) for node_id, release_node in release.values.items()
    }
