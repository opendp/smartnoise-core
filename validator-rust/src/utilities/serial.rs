//! Serialization and deserialization between prost protobuf structs and internal representations

use crate::{proto, Integer, Float};
use std::collections::HashMap;
use crate::base::{Release, Nature, Jagged, Vector1D, Value, Array, Vector1DNull, NatureCategorical, NatureContinuous, AggregatorProperties, ValueProperties, JaggedProperties, DataType, ArrayProperties, ReleaseNode, GroupId, IndexKey, ComponentExpansion, DataframeProperties, PartitionsProperties};
use indexmap::IndexMap;
use error_chain::ChainedError;

// PARSERS
pub fn parse_bool_null(value: proto::BoolNull) -> Option<bool> {
    value.data.map(|elem_data|
        match elem_data { proto::bool_null::Data::Option(x) => x })
}

pub fn parse_i64_null(value: proto::I64Null) -> Option<i64> {
    value.data.map(|elem_data|
        match elem_data { proto::i64_null::Data::Option(x) => x })
}

pub fn parse_f64_null(value: proto::F64Null) -> Option<f64> {
    value.data.map(|elem_data|
        match elem_data { proto::f64_null::Data::Option(x) => x })
}

pub fn parse_str_null(value: proto::StrNull) -> Option<String> {
    value.data.map(|elem_data|
        match elem_data { proto::str_null::Data::Option(x) => x })
}


pub fn parse_array1d_bool_null(value: proto::Array1dBoolNull) -> Vec<Option<bool>> {
    value.data.into_iter().map(parse_bool_null).collect()
}

pub fn parse_array1d_i64_null(value: proto::Array1dI64Null) -> Vec<Option<i64>> {
    value.data.into_iter().map(parse_i64_null).collect()
}

pub fn parse_array1d_f64_null(value: proto::Array1dF64Null) -> Vec<Option<f64>> {
    value.data.into_iter().map(parse_f64_null).collect()
}

pub fn parse_array1d_str_null(value: proto::Array1dStrNull) -> Vec<Option<String>> {
    value.data.into_iter().map(parse_str_null).collect()
}

pub fn parse_array1d_null(value: proto::Array1dNull) -> Vector1DNull {
    match value.data.unwrap() {
        proto::array1d_null::Data::Bool(vector) => Vector1DNull::Bool(parse_array1d_bool_null(vector)),
        proto::array1d_null::Data::String(vector) => Vector1DNull::Str(parse_array1d_str_null(vector)),
        proto::array1d_null::Data::I64(vector) => Vector1DNull::Int(parse_array1d_i64_null(vector)
            .into_iter().map(|v| v.map(|v| v as Integer)).collect()),
        proto::array1d_null::Data::F64(vector) => Vector1DNull::Float(parse_array1d_f64_null(vector)
            .into_iter().map(|v| v.map(|v| v as Float)).collect()),
    }
}


pub fn parse_array1d_bool(value: proto::Array1dBool) -> Vec<bool> { value.data }

// PERF: can use conditional compilation to remove the loop when type matches proto
pub fn parse_array1d_i64(value: proto::Array1dI64) -> Vec<i64> { value.data }

pub fn parse_array1d_f64(value: proto::Array1dF64) -> Vec<f64> { value.data }

pub fn parse_array1d_str(value: proto::Array1dStr) -> Vec<String> { value.data }


pub fn parse_array1d(value: proto::Array1d) -> Vector1D {
    match value.data.unwrap() {
        proto::array1d::Data::Bool(vector) => Vector1D::Bool(parse_array1d_bool(vector)),
        proto::array1d::Data::String(vector) => Vector1D::Str(parse_array1d_str(vector)),
        proto::array1d::Data::I64(vector) => Vector1D::Int(parse_array1d_i64(vector)
            .into_iter().map(|v| v as Integer).collect()),
        proto::array1d::Data::F64(vector) => Vector1D::Float(parse_array1d_f64(vector)
            .into_iter().map(|v| v as Float).collect()),
    }
}


pub fn parse_array(value: proto::Array) -> Array {
    let shape: Vec<usize> = value.shape.into_iter().map(|x| x as usize).collect();
    match parse_array1d(value.flattened.unwrap()) {
        Vector1D::Bool(vector) => Array::Bool(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Int(vector) => Array::Int(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Float(vector) => Array::Float(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Str(vector) => Array::Str(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
    }
}

pub fn parse_dataframe(value: proto::Dataframe) -> IndexMap<IndexKey, Value> {
    let proto::Dataframe { keys, values } = value;
    keys.into_iter()
        .zip(values.into_iter())
        .map(|(k, v)| (parse_index_key(k), parse_value(v)))
        .collect()
}

pub fn parse_partitions(value: proto::Partitions) -> IndexMap<IndexKey, Value> {
    let proto::Partitions { keys, values } = value;
    keys.into_iter()
        .zip(values.into_iter())
        .map(|(k, v)| (parse_index_key(k), parse_value(v)))
        .collect()
}

pub fn parse_data_type(value: proto::DataType) -> DataType {
    match value {
        proto::DataType::Unknown => DataType::Unknown,
        proto::DataType::Bool => DataType::Bool,
        proto::DataType::F64 => DataType::Float,
        proto::DataType::I64 => DataType::Int,
        proto::DataType::String => DataType::Str,
    }
}

pub fn parse_jagged(value: proto::Jagged) -> Jagged {
    match proto::DataType::from_i32(value.data_type).unwrap() {
        proto::DataType::Unknown => panic!("data type of Jagged must be known"),
        proto::DataType::Bool => Jagged::Bool(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::Bool(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<bool>>>()),
        proto::DataType::F64 => Jagged::Float(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::Float(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<Float>>>()),
        proto::DataType::I64 => Jagged::Int(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::Int(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<Integer>>>()),
        proto::DataType::String => Jagged::Str(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::Str(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<String>>>()),
    }
}

pub fn parse_value(value: proto::Value) -> Value {
    match value.data.unwrap() {
        proto::value::Data::Function(function) =>
            Value::Function(function),
        proto::value::Data::Array(data) =>
            Value::Array(parse_array(data)),
        proto::value::Data::Dataframe(data) =>
            Value::Dataframe(parse_dataframe(data)),
        proto::value::Data::Partitions(data) =>
            Value::Partitions(parse_partitions(data)),
        proto::value::Data::Jagged(data) =>
            Value::Jagged(parse_jagged(data))
    }
}

pub fn parse_release(release: proto::Release) -> Release {
    release.values.into_iter().map(|(idx, release_node)| (idx, parse_release_node(release_node)))
        .collect::<HashMap<u32, ReleaseNode>>()
}

pub fn parse_release_node(release_node: proto::ReleaseNode) -> ReleaseNode {
    ReleaseNode {
        value: parse_value(release_node.value.unwrap()),
        privacy_usages: release_node.privacy_usages.map(|v| v.values),
        public: release_node.public
    }
}

pub fn parse_value_properties(value: proto::ValueProperties) -> ValueProperties {
    match value.variant.unwrap() {
        proto::value_properties::Variant::Dataframe(value) =>
            ValueProperties::Dataframe(parse_dataframe_properties(value)),
        proto::value_properties::Variant::Partitions(value) =>
            ValueProperties::Partitions(parse_partitions_properties(value)),
        proto::value_properties::Variant::Array(value) =>
            ValueProperties::Array(parse_array_properties(value)),
        proto::value_properties::Variant::Jagged(value) =>
            ValueProperties::Jagged(parse_jagged_properties(value)),
        proto::value_properties::Variant::Function(value) =>
            ValueProperties::Function(value),
    }
}

pub fn parse_argument_node_ids(value: proto::ArgumentNodeIds) -> IndexMap<IndexKey, u32> {
    value.values.iter().zip(value.keys.into_iter())
        .map(|(v, k)| (parse_index_key(k), *v))
        .collect()
}

pub fn parse_indexmap_release_node(value: proto::IndexmapReleaseNode) -> IndexMap<IndexKey, ReleaseNode> {
    value.keys.iter().zip(value.values.into_iter())
        .map(|(k, v)|
            (parse_index_key(k.clone()), parse_release_node(v)))
        .collect()
}

pub fn parse_partitions_properties(value: proto::PartitionsProperties) -> PartitionsProperties {
    let proto::PartitionsProperties { keys, values } = value;
    PartitionsProperties {
        children: keys.into_iter().zip(values.into_iter())
            .map(|(k, v)| (parse_index_key(k), parse_value_properties(v)))
            .collect(),
    }
}

pub fn parse_dataframe_properties(value: proto::DataframeProperties) -> DataframeProperties {
    let proto::DataframeProperties { keys, values } = value;
    DataframeProperties {
        children: keys.into_iter().zip(values.into_iter())
            .map(|(k, v)| (parse_index_key(k), parse_value_properties(v)))
            .collect(),
    }
}

pub fn parse_argument_properties(value: proto::ArgumentProperties) -> IndexMap<IndexKey, ValueProperties> {
    let proto::ArgumentProperties { keys, values } = value;
    keys.into_iter().zip(values.into_iter())
        .map(|(k, v)| (parse_index_key(k), parse_value_properties(v)))
        .collect()
}

pub fn parse_index_key(value: proto::IndexKey) -> IndexKey {
    match value.key.unwrap() {
        proto::index_key::Key::Str(key) => IndexKey::Str(key),
        proto::index_key::Key::Bool(key) => IndexKey::Bool(key),
        proto::index_key::Key::I64(key) => IndexKey::Int(key as Integer),
        proto::index_key::Key::Tuple(key) => IndexKey::Tuple(key.values.into_iter().map(parse_index_key).collect())
    }
}

pub fn parse_group_id(value: proto::GroupId) -> GroupId {
    GroupId {
        partition_id: value.partition_id,
        index: parse_index_key(value.index.unwrap())
    }
}

pub fn parse_array_properties(value: proto::ArrayProperties) -> ArrayProperties {
    ArrayProperties {
        num_records: value.num_records.and_then(parse_i64_null),
        num_columns: value.num_columns.and_then(parse_i64_null),
        nullity: value.nullity,
        releasable: value.releasable,
        c_stability: parse_array1d_f64(value.c_stability.to_owned().unwrap())
            .into_iter().map(|v| v as Float).collect(),
        aggregator: value.aggregator.map(|aggregator| AggregatorProperties {
            component: aggregator.component.unwrap().variant.unwrap(),
            properties: parse_argument_properties(aggregator.properties.unwrap()),
            lipschitz_constants: parse_value(aggregator.lipschitz_constants.unwrap())
        }),
        nature: value.nature.map(|nature| match nature {
            proto::array_properties::Nature::Continuous(continuous) =>
                Nature::Continuous(NatureContinuous {
                    lower: parse_array1d_null(continuous.minimum.unwrap()),
                    upper: parse_array1d_null(continuous.maximum.unwrap()),
                }),
            proto::array_properties::Nature::Categorical(categorical) =>
                Nature::Categorical(NatureCategorical {
                    categories: parse_jagged(categorical.categories.unwrap())
                })
        }),
        data_type: parse_data_type(proto::DataType::from_i32(value.data_type).unwrap()),
        dataset_id: value.dataset_id.and_then(parse_i64_null),
        is_not_empty: value.is_not_empty,
        dimensionality: value.dimensionality.and_then(parse_i64_null),
        group_id: value.group_id.into_iter().map(parse_group_id).collect(),
        naturally_ordered: value.naturally_ordered
    }
}

pub fn parse_jagged_properties(value: proto::JaggedProperties) -> JaggedProperties {
    JaggedProperties {
        num_records: value.num_records.map(parse_array1d_i64),
        nullity: value.nullity,
        releasable: value.releasable,
        aggregator: value.aggregator.map(|aggregator| AggregatorProperties {
            component: aggregator.component.unwrap().variant.unwrap(),
            properties: parse_argument_properties(aggregator.properties.unwrap()),
            lipschitz_constants: parse_value(aggregator.lipschitz_constants.unwrap())
        }),
        nature: value.nature.map(|nature| match nature {
            proto::jagged_properties::Nature::Continuous(continuous) =>
                Nature::Continuous(NatureContinuous {
                    lower: parse_array1d_null(continuous.minimum.unwrap()),
                    upper: parse_array1d_null(continuous.maximum.unwrap()),
                }),
            proto::jagged_properties::Nature::Categorical(categorical) =>
                Nature::Categorical(NatureCategorical {
                    categories: parse_jagged(categorical.categories.unwrap())
                })
        }),
        data_type: parse_data_type(proto::DataType::from_i32(value.data_type).unwrap()),
    }
}


// SERIALIZERS
pub fn serialize_bool_null(value: Option<bool>) -> proto::BoolNull {
    proto::BoolNull {
        data: value.map(proto::bool_null::Data::Option)
    }
}

pub fn serialize_i64_null(value: Option<i64>) -> proto::I64Null {
    proto::I64Null {
        data: value.map(proto::i64_null::Data::Option)
    }
}

pub fn serialize_f64_null(value: Option<f64>) -> proto::F64Null {
    proto::F64Null {
        data: value.map(proto::f64_null::Data::Option)
    }
}

pub fn serialize_str_null(value: Option<String>) -> proto::StrNull {
    proto::StrNull {
        data: value.map(proto::str_null::Data::Option)
    }
}


pub fn serialize_array1d_bool_null(value: Vec<Option<bool>>) -> proto::Array1dBoolNull {
    proto::Array1dBoolNull {
        data: value.into_iter().map(serialize_bool_null).collect()
    }
}

pub fn serialize_array1d_i64_null(value: Vec<Option<i64>>) -> proto::Array1dI64Null {
    proto::Array1dI64Null {
        data: value.into_iter().map(serialize_i64_null).collect()
    }
}

pub fn serialize_array1d_f64_null(value: Vec<Option<f64>>) -> proto::Array1dF64Null {
    proto::Array1dF64Null {
        data: value.into_iter().map(serialize_f64_null).collect()
    }
}

pub fn serialize_array1d_str_null(value: Vec<Option<String>>) -> proto::Array1dStrNull {
    proto::Array1dStrNull {
        data: value.into_iter().map(serialize_str_null).collect()
    }
}


pub fn serialize_array1d_null(value: Vector1DNull) -> proto::Array1dNull {
    proto::Array1dNull {
        data: Some(match value {
            Vector1DNull::Bool(vector) => proto::array1d_null::Data::Bool(serialize_array1d_bool_null(vector)),
            Vector1DNull::Str(vector) => proto::array1d_null::Data::String(serialize_array1d_str_null(vector)),
            Vector1DNull::Int(vector) => proto::array1d_null::Data::I64(serialize_array1d_i64_null(vector
                .into_iter().map(|v| v.map(|v| v as i64)).collect())),
            Vector1DNull::Float(vector) => proto::array1d_null::Data::F64(serialize_array1d_f64_null(vector
                .into_iter().map(|v| v.map(|v| v as f64)).collect())),
        })
    }
}


pub fn serialize_array1d_bool(value: Vec<bool>) -> proto::Array1dBool { proto::Array1dBool { data: value } }

pub fn serialize_array1d_i64(value: Vec<i64>) -> proto::Array1dI64 { proto::Array1dI64 { data: value } }

pub fn serialize_array1d_f64(value: Vec<f64>) -> proto::Array1dF64 { proto::Array1dF64 { data: value } }

pub fn serialize_array1d_str(value: Vec<String>) -> proto::Array1dStr { proto::Array1dStr { data: value } }


pub fn serialize_array1d(value: Vector1D) -> proto::Array1d {
    proto::Array1d {
        data: Some(match value {
            Vector1D::Bool(vector) => proto::array1d::Data::Bool(serialize_array1d_bool(vector)),
            Vector1D::Str(vector) => proto::array1d::Data::String(serialize_array1d_str(vector)),
            Vector1D::Int(vector) => proto::array1d::Data::I64(serialize_array1d_i64(vector
                .into_iter().map(|v| v as i64).collect())),
            Vector1D::Float(vector) => proto::array1d::Data::F64(serialize_array1d_f64(vector
                .into_iter().map(|v| v as f64).collect())),
        })
    }
}

pub fn serialize_array(value: Array) -> proto::Array {
    match value {
        Array::Bool(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Bool(array.iter().copied().collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Float(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Float(array.iter().copied().collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Int(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Int(array.iter().copied().collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Str(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Str(array.iter().cloned().collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        }
    }
}

pub fn serialize_partitions(value: IndexMap<IndexKey, Value>) -> proto::Partitions {
    proto::Partitions {
        keys: value.keys().map(|k| serialize_index_key(k.clone())).collect(),
        values: value.into_iter().map(|(_, v)| serialize_value(v)).collect()
    }
}

pub fn serialize_dataframe(value: IndexMap<IndexKey, Value>) -> proto::Dataframe {
    proto::Dataframe {
        keys: value.keys().map(|k| serialize_index_key(k.clone())).collect(),
        values: value.into_iter().map(|(_, v)| serialize_value(v)).collect()
    }
}

pub fn serialize_data_type(value: DataType) -> proto::DataType {
    match value {
        DataType::Unknown => proto::DataType::Unknown,
        DataType::Bool => proto::DataType::Bool,
        DataType::Float => proto::DataType::F64,
        DataType::Int => proto::DataType::I64,
        DataType::Str => proto::DataType::String,
    }
}

pub fn serialize_jagged(value: Jagged) -> proto::Jagged {
    proto::Jagged {
        data_type: match &value {
            Jagged::Bool(_x) => proto::DataType::Bool as i32,
            Jagged::Float(_x) => proto::DataType::F64 as i32,
            Jagged::Int(_x) => proto::DataType::I64 as i32,
            Jagged::Str(_x) => proto::DataType::String as i32,
        },
        data: match value {
            Jagged::Bool(data) => data.into_iter().map(Vector1D::Bool).map(serialize_array1d).collect(),
            Jagged::Float(data) => data.into_iter().map(Vector1D::Float).map(serialize_array1d).collect(),
            Jagged::Int(data) => data.into_iter().map(Vector1D::Int).map(serialize_array1d).collect(),
            Jagged::Str(data) => data.into_iter().map(Vector1D::Str).map(serialize_array1d).collect(),
        },
    }
}

pub fn serialize_value(value: Value) -> proto::Value {
    proto::Value {
        data: Some(match value {
            Value::Function(data) =>
                proto::value::Data::Function(data),
            Value::Array(data) =>
                proto::value::Data::Array(serialize_array(data)),
            Value::Partitions(data) =>
                proto::value::Data::Partitions(serialize_partitions(data)),
            Value::Dataframe(data) =>
                proto::value::Data::Dataframe(serialize_dataframe(data)),
            Value::Jagged(data) =>
                proto::value::Data::Jagged(serialize_jagged(data))
        })
    }
}

pub fn serialize_release(release: Release) -> proto::Release {
    proto::Release {
        values: release.into_iter()
            .map(|(idx, release_node)| (idx, serialize_release_node(release_node)))
            .collect::<HashMap<u32, proto::ReleaseNode>>()
    }
}

pub fn serialize_release_node(release_node: ReleaseNode) -> proto::ReleaseNode {
    proto::ReleaseNode {
        value: Some(serialize_value(release_node.value)),
        privacy_usages: release_node.privacy_usages.map(|v| proto::PrivacyUsages { values: v }),
        public: release_node.public
    }
}

pub fn serialize_argument_node_ids(value: IndexMap<IndexKey, u32>) -> proto::ArgumentNodeIds {
    proto::ArgumentNodeIds {
        values: value.values().cloned().collect(),
        keys: value.into_iter().map(|v| v.0).map(serialize_index_key).collect(),
    }
}

pub fn serialize_argument_properties(value: IndexMap<IndexKey, ValueProperties>) -> proto::ArgumentProperties {
    proto::ArgumentProperties {
        keys: value.keys().cloned().map(serialize_index_key).collect(),
        values: value.into_iter().map(|v| v.1).map(serialize_value_properties).collect()
    }
}

pub fn serialize_dataframe_properties(value: DataframeProperties) -> proto::DataframeProperties {
    proto::DataframeProperties {
        keys: value.children.keys().cloned().map(serialize_index_key).collect(),
        values: value.children.into_iter().map(|v| v.1).map(serialize_value_properties).collect()
    }
}

pub fn serialize_partitions_properties(value: PartitionsProperties) -> proto::PartitionsProperties {
    proto::PartitionsProperties {
        keys: value.children.keys().cloned().map(serialize_index_key).collect(),
        values: value.children.into_iter().map(|v| v.1).map(serialize_value_properties).collect()
    }
}

pub fn serialize_index_key(value: IndexKey) -> proto::IndexKey {
    proto::IndexKey {
        key: Some(match value {
            IndexKey::Str(key) => proto::index_key::Key::Str(key),
            IndexKey::Bool(key) => proto::index_key::Key::Bool(key),
            IndexKey::Int(key) => proto::index_key::Key::I64(key as i64),
            IndexKey::Tuple(key) =>
                proto::index_key::Key::Tuple(proto::index_key::Tuple {
                    values: key.into_iter().map(serialize_index_key).collect()
                })
        })
    }
}

pub fn serialize_group_id(value: GroupId) -> proto::GroupId {
    proto::GroupId {
        partition_id: value.partition_id,
        index: Some(serialize_index_key(value.index))
    }
}

pub fn serialize_array_properties(value: ArrayProperties) -> proto::ArrayProperties {

    let ArrayProperties {
        num_records, num_columns, nullity, releasable,
        c_stability, aggregator, nature,
        data_type, dataset_id, is_not_empty,
        dimensionality, group_id, naturally_ordered
    } = value;

    proto::ArrayProperties {
        num_records: Some(serialize_i64_null(num_records)),
        num_columns: Some(serialize_i64_null(num_columns)),
        nullity,
        releasable,
        c_stability: Some(serialize_array1d_f64(c_stability.into_iter().map(|v| v as f64).collect())),
        nature: nature.map(|nature| match nature {
            Nature::Categorical(categorical) => proto::array_properties::Nature::Categorical(proto::NatureCategorical {
                categories: Some(serialize_jagged(categorical.categories))
            }),
            Nature::Continuous(x) => proto::array_properties::Nature::Continuous(proto::NatureContinuous {
                minimum: Some(serialize_array1d_null(x.lower)),
                maximum: Some(serialize_array1d_null(x.upper)),
            })
        }),
        aggregator: aggregator.map(|aggregator| proto::AggregatorProperties {
            // the component here is just a vessel to serialize the variant
            component: Some(proto::Component {
                variant: Some(aggregator.component),
                omit: true,
                submission: 0,
                arguments: Some(proto::ArgumentNodeIds::default()),
            }),
            properties: Some(serialize_argument_properties(aggregator.properties)),
            lipschitz_constants: Some(serialize_value(aggregator.lipschitz_constants)),
        }),
        data_type: serialize_data_type(data_type) as i32,
        dataset_id: Some(serialize_i64_null(dataset_id)),
        is_not_empty,
        dimensionality: Some(serialize_i64_null(dimensionality)),
        group_id: group_id.into_iter().map(serialize_group_id).collect(),
        naturally_ordered
    }
}

pub fn serialize_jagged_properties(value: JaggedProperties) -> proto::JaggedProperties {
    let JaggedProperties {
        num_records, nullity, aggregator, nature, data_type, releasable
    } = value;

    proto::JaggedProperties {
        num_records: num_records.map(serialize_array1d_i64),
        nullity,
        releasable,
        nature: nature.map(|nature| match nature {
            Nature::Categorical(categorical) => proto::jagged_properties::Nature::Categorical(proto::NatureCategorical {
                categories: Some(serialize_jagged(categorical.categories))
            }),
            Nature::Continuous(x) => proto::jagged_properties::Nature::Continuous(proto::NatureContinuous {
                minimum: Some(serialize_array1d_null(x.lower)),
                maximum: Some(serialize_array1d_null(x.upper)),
            })
        }),
        aggregator: aggregator.map(|aggregator| proto::AggregatorProperties {
            component: Some(proto::Component {
                variant: Some(aggregator.component),
                omit: true,
                submission: 0,
                arguments: Some(proto::ArgumentNodeIds::default()),
            }),
            properties: Some(serialize_argument_properties(aggregator.properties)),
            lipschitz_constants: Some(serialize_value(aggregator.lipschitz_constants))
        }),
        data_type: serialize_data_type(data_type) as i32
    }
}

pub fn serialize_value_properties(value: ValueProperties) -> proto::ValueProperties {
    proto::ValueProperties {
        variant: Some(match value {
            ValueProperties::Partitions(value) =>
                proto::value_properties::Variant::Partitions(serialize_partitions_properties(value)),
            ValueProperties::Dataframe(value) =>
                proto::value_properties::Variant::Dataframe(serialize_dataframe_properties(value)),
            ValueProperties::Array(value) =>
                proto::value_properties::Variant::Array(serialize_array_properties(value)),
            ValueProperties::Jagged(value) =>
                proto::value_properties::Variant::Jagged(serialize_jagged_properties(value)),
            ValueProperties::Function(value) => proto::value_properties::Variant::Function(value)
        })
    }
}

pub fn serialize_component_expansion(value: ComponentExpansion) -> proto::ComponentExpansion {
    proto::ComponentExpansion {
        computation_graph: value.computation_graph,
        properties: value.properties.into_iter()
            .map(|(node_id, property)|
                (node_id, serialize_value_properties(property)))
            .collect(),
        releases: value.releases.into_iter()
            .map(|(node_id, release)|
                (node_id, serialize_release_node(release)))
            .collect(),
        traversal: value.traversal,
        warnings: value.warnings.into_iter()
            .map(serialize_error).collect()
    }
}

#[doc(hidden)]
pub fn serialize_error(err: crate::Error) -> proto::Error {
    proto::Error { message: err.display_chain().to_string() }
}