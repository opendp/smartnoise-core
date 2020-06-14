//! Serialization and deserialization between prost protobuf structs and internal representations

use crate::{proto, base};
use std::collections::HashMap;
use crate::base::{
    Release, Nature, Jagged, Vector1D, Value, Array, Vector1DNull,
    NatureCategorical, NatureContinuous, AggregatorProperties, ValueProperties,
    IndexmapProperties, JaggedProperties, DataType, ArrayProperties, ReleaseNode,
    GroupId, IndexKey
};
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
        proto::array1d_null::Data::I64(vector) => Vector1DNull::I64(parse_array1d_i64_null(vector)),
        proto::array1d_null::Data::F64(vector) => Vector1DNull::F64(parse_array1d_f64_null(vector)),
        proto::array1d_null::Data::String(vector) => Vector1DNull::Str(parse_array1d_str_null(vector)),
    }
}


pub fn parse_array1d_bool(value: proto::Array1dBool) -> Vec<bool> { value.data }

pub fn parse_array1d_i64(value: proto::Array1dI64) -> Vec<i64> { value.data }

pub fn parse_array1d_f64(value: proto::Array1dF64) -> Vec<f64> { value.data }

pub fn parse_array1d_str(value: proto::Array1dStr) -> Vec<String> { value.data }


pub fn parse_array1d(value: proto::Array1d) -> Vector1D {
    match value.data.unwrap() {
        proto::array1d::Data::Bool(vector) => Vector1D::Bool(parse_array1d_bool(vector)),
        proto::array1d::Data::I64(vector) => Vector1D::I64(parse_array1d_i64(vector)),
        proto::array1d::Data::F64(vector) => Vector1D::F64(parse_array1d_f64(vector)),
        proto::array1d::Data::String(vector) => Vector1D::Str(parse_array1d_str(vector)),
    }
}


pub fn parse_array(value: proto::Array) -> Array {
    let shape: Vec<usize> = value.shape.into_iter().map(|x| x as usize).collect();
    match parse_array1d(value.flattened.unwrap()) {
        Vector1D::Bool(vector) => Array::Bool(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::I64(vector) => Array::I64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::F64(vector) => Array::F64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Str(vector) => Array::Str(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
    }
}

pub fn parse_indexmap(value: proto::Indexmap) -> IndexMap<IndexKey, Value> {
    let proto::Indexmap { keys, values } = value;
    keys.into_iter()
        .zip(values.into_iter())
        .map(|(k, v)| (parse_index_key(k), parse_value(v)))
        .collect()
}

pub fn parse_data_type(value: proto::DataType) -> DataType {
    match value {
        proto::DataType::Unknown => DataType::Unknown,
        proto::DataType::Bool => DataType::Bool,
        proto::DataType::F64 => DataType::F64,
        proto::DataType::I64 => DataType::I64,
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
        proto::DataType::F64 => Jagged::F64(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::F64(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<f64>>>()),
        proto::DataType::I64 => Jagged::I64(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::I64(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<i64>>>()),
        proto::DataType::String => Jagged::Str(value.data.into_iter()
            .map(|column| match parse_array1d(column) {
                Vector1D::Str(vector) => vector,
                _ => panic!()
            }).collect::<Vec<Vec<String>>>()),
    }
}

pub fn parse_value(value: proto::Value) -> Value {
    match value.data.to_owned().unwrap() {
        proto::value::Data::Function(function) =>
            Value::Function(function),
        proto::value::Data::Array(data) =>
            Value::Array(parse_array(data)),
        proto::value::Data::Indexmap(data) =>
            Value::Indexmap(parse_indexmap(data)),
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
        proto::value_properties::Variant::Indexmap(value) =>
            ValueProperties::Indexmap(parse_indexmap_properties(value)),
        proto::value_properties::Variant::Array(value) =>
            ValueProperties::Array(parse_array_properties(value)),
        proto::value_properties::Variant::Jagged(value) =>
            ValueProperties::Jagged(parse_jagged_properties(value)),
        proto::value_properties::Variant::Function(value) =>
            ValueProperties::Function(value),
    }
}

pub fn parse_indexmap_node_ids(value: proto::IndexmapNodeIds) -> IndexMap<IndexKey, u32> {
    value.values.iter().zip(value.keys.into_iter())
        .map(|(v, k)| (parse_index_key(k), *v))
        .collect()
}

pub fn parse_indexmap_value_properties(value: proto::IndexmapValueProperties) -> IndexMap<IndexKey, ValueProperties> {
    let proto::IndexmapValueProperties { keys, values } = value;
    keys.into_iter().zip(values.into_iter())
        .map(|(k, v)| (parse_index_key(k), parse_value_properties(v)))
        .collect()
}

pub fn parse_indexmap_properties(value: proto::IndexmapProperties) -> IndexmapProperties {
    IndexmapProperties {
        children: parse_indexmap_value_properties(value.children.unwrap()),
        variant: proto::indexmap_properties::Variant::from_i32(value.variant).unwrap(),
    }
}

pub fn parse_index_key(value: proto::IndexKey) -> base::IndexKey {
    match value.key.unwrap() {
        proto::index_key::Key::Str(key) => base::IndexKey::Str(key),
        proto::index_key::Key::Bool(key) => base::IndexKey::Bool(key),
        proto::index_key::Key::I64(key) => base::IndexKey::I64(key),
        proto::index_key::Key::Tuple(key) => base::IndexKey::Tuple(key.values.into_iter().map(parse_index_key).collect())
    }
}

pub fn parse_group_id(value: proto::GroupId) -> GroupId {
    GroupId {
        partition_id: value.partition_id,
        index: value.index.map(|idx| parse_index_key(idx))
    }
}

pub fn parse_array_properties(value: proto::ArrayProperties) -> ArrayProperties {
    ArrayProperties {
        num_records: value.num_records.and_then(parse_i64_null),
        num_columns: value.num_columns.and_then(parse_i64_null),
        nullity: value.nullity,
        releasable: value.releasable,
        c_stability: parse_array1d_f64(value.c_stability.to_owned().unwrap()),
        aggregator: value.aggregator.map(|aggregator| AggregatorProperties {
            component: aggregator.component.unwrap().variant.unwrap(),
            properties: parse_indexmap_value_properties(aggregator.properties.unwrap()),
            c_stability: parse_array1d_f64(aggregator.c_stability.unwrap()),
            lipschitz_constant: parse_array1d_f64(aggregator.lipschitz_constant.unwrap())
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
        group_id: value.group_id.into_iter().map(parse_group_id).collect()
    }
}

pub fn parse_jagged_properties(value: proto::JaggedProperties) -> JaggedProperties {
    JaggedProperties {
        num_records: value.num_records.map(parse_array1d_i64),
        nullity: value.nullity,
        releasable: value.releasable,
        aggregator: value.aggregator.map(|aggregator| AggregatorProperties {
            component: aggregator.component.unwrap().variant.unwrap(),
            properties: parse_indexmap_value_properties(aggregator.properties.unwrap()),
            c_stability: parse_array1d_f64(aggregator.c_stability.unwrap()),
            lipschitz_constant: parse_array1d_f64(aggregator.lipschitz_constant.unwrap())
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
        data: value.map(|elem_data| proto::bool_null::Data::Option(elem_data))
    }
}

pub fn serialize_i64_null(value: Option<i64>) -> proto::I64Null {
    proto::I64Null {
        data: value.map(|elem_data| proto::i64_null::Data::Option(elem_data))
    }
}

pub fn serialize_f64_null(value: Option<f64>) -> proto::F64Null {
    proto::F64Null {
        data: value.map(|elem_data| proto::f64_null::Data::Option(elem_data))
    }
}

pub fn serialize_str_null(value: Option<String>) -> proto::StrNull {
    proto::StrNull {
        data: value.map(|elem_data| proto::str_null::Data::Option(elem_data))
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
            Vector1DNull::I64(vector) => proto::array1d_null::Data::I64(serialize_array1d_i64_null(vector)),
            Vector1DNull::F64(vector) => proto::array1d_null::Data::F64(serialize_array1d_f64_null(vector)),
            Vector1DNull::Str(vector) => proto::array1d_null::Data::String(serialize_array1d_str_null(vector)),
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
            Vector1D::I64(vector) => proto::array1d::Data::I64(serialize_array1d_i64(vector)),
            Vector1D::F64(vector) => proto::array1d::Data::F64(serialize_array1d_f64(vector)),
            Vector1D::Str(vector) => proto::array1d::Data::String(serialize_array1d_str(vector)),
        })
    }
}

pub fn serialize_array(value: Array) -> proto::Array {
    match value {
        Array::Bool(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Bool(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::F64(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::F64(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::I64(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::I64(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Str(array) => proto::Array {
            flattened: Some(serialize_array1d(Vector1D::Str(array.iter().map(|s| s.to_owned()).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        }
    }
}

pub fn serialize_indexmap(value: IndexMap<IndexKey, Value>) -> proto::Indexmap {
    proto::Indexmap {
        keys: value.keys().map(|k| serialize_index_key(k.clone())).collect(),
        values: value.into_iter().map(|(_, v)| serialize_value(v)).collect()
    }
}

pub fn serialize_data_type(value: DataType) -> proto::DataType {
    match value {
        DataType::Unknown => proto::DataType::Unknown,
        DataType::Bool => proto::DataType::Bool,
        DataType::F64 => proto::DataType::F64,
        DataType::I64 => proto::DataType::I64,
        DataType::Str => proto::DataType::String,
    }
}

pub fn serialize_jagged(value: Jagged) -> proto::Jagged {
    proto::Jagged {
        data_type: match &value {
            Jagged::Bool(_x) => proto::DataType::Bool as i32,
            Jagged::F64(_x) => proto::DataType::F64 as i32,
            Jagged::I64(_x) => proto::DataType::I64 as i32,
            Jagged::Str(_x) => proto::DataType::String as i32,
        },
        data: match value {
            Jagged::Bool(data) => data.into_iter().map(Vector1D::Bool).map(serialize_array1d).collect(),
            Jagged::F64(data) => data.into_iter().map(Vector1D::F64).map(serialize_array1d).collect(),
            Jagged::I64(data) => data.into_iter().map(Vector1D::I64).map(serialize_array1d).collect(),
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
            Value::Indexmap(data) =>
                proto::value::Data::Indexmap(serialize_indexmap(data)),
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

pub fn serialize_indexmap_node_ids(value: IndexMap<IndexKey, u32>) -> proto::IndexmapNodeIds {
    proto::IndexmapNodeIds {
        values: value.values().cloned().collect(),
        keys: value.into_iter().map(|v| v.0).map(serialize_index_key).collect(),
    }
}

pub fn serialize_indexmap_value_properties(value: IndexMap<IndexKey, ValueProperties>) -> proto::IndexmapValueProperties {
    proto::IndexmapValueProperties {
        keys: value.keys().cloned().map(serialize_index_key).collect(),
        values: value.into_iter().map(|v| v.1).map(serialize_value_properties).collect()
    }
}

pub fn serialize_indexmap_properties(value: IndexmapProperties) -> proto::IndexmapProperties {
    proto::IndexmapProperties {
        children: Some(proto::IndexmapValueProperties {
            keys: value.children.keys().cloned().map(serialize_index_key).collect(),
            values: value.children.into_iter().map(|v| v.1).map(serialize_value_properties).collect()
        }),
        variant: value.variant as i32
    }
}

pub fn serialize_index_key(value: base::IndexKey) -> proto::IndexKey {
    proto::IndexKey {
        key: Some(match value {
            base::IndexKey::Str(key) => proto::index_key::Key::Str(key),
            base::IndexKey::Bool(key) => proto::index_key::Key::Bool(key),
            base::IndexKey::I64(key) => proto::index_key::Key::I64(key),
            base::IndexKey::Tuple(key) =>
                proto::index_key::Key::Tuple(proto::index_key::Tuple {
                    values: key.into_iter().map(serialize_index_key).collect()
                })
        })
    }
}

pub fn serialize_group_id(value: GroupId) -> proto::GroupId {
    proto::GroupId {
        partition_id: value.partition_id,
        index: value.index.map(|idx| serialize_index_key(idx))
    }
}

pub fn serialize_array_properties(value: ArrayProperties) -> proto::ArrayProperties {

    let ArrayProperties {
        num_records, num_columns, nullity, releasable,
        c_stability, aggregator, nature,
        data_type, dataset_id, is_not_empty,
        dimensionality, group_id
    } = value;

    proto::ArrayProperties {
        num_records: Some(serialize_i64_null(num_records)),
        num_columns: Some(serialize_i64_null(num_columns)),
        nullity,
        releasable,
        c_stability: Some(serialize_array1d_f64(c_stability)),
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
                arguments: Some(proto::IndexmapNodeIds::default()),
            }),
            properties: Some(serialize_indexmap_value_properties(aggregator.properties)),
            lipschitz_constant: Some(serialize_array1d_f64(aggregator.lipschitz_constant)),
            c_stability: Some(serialize_array1d_f64(aggregator.c_stability)),
        }),
        data_type: serialize_data_type(data_type) as i32,
        dataset_id: Some(serialize_i64_null(dataset_id)),
        is_not_empty,
        dimensionality: Some(serialize_i64_null(dimensionality)),
        group_id: group_id.into_iter().map(serialize_group_id).collect()
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
                arguments: Some(proto::IndexmapNodeIds::default()),
            }),
            properties: Some(serialize_indexmap_value_properties(aggregator.properties)),
            c_stability: Some(serialize_array1d_f64(aggregator.c_stability)),
            lipschitz_constant: Some(serialize_array1d_f64(aggregator.lipschitz_constant))
        }),
        data_type: serialize_data_type(data_type) as i32
    }
}

pub fn serialize_value_properties(value: ValueProperties) -> proto::ValueProperties {
    proto::ValueProperties {
        variant: Some(match value {
            ValueProperties::Indexmap(value) =>
                proto::value_properties::Variant::Indexmap(serialize_indexmap_properties(value)),
            ValueProperties::Array(value) =>
                proto::value_properties::Variant::Array(serialize_array_properties(value)),
            ValueProperties::Jagged(value) =>
                proto::value_properties::Variant::Jagged(serialize_jagged_properties(value)),
            ValueProperties::Function(value) => proto::value_properties::Variant::Function(value)
        })
    }
}

#[doc(hidden)]
pub fn serialize_error(err: crate::Error) -> proto::Error {
    proto::Error { message: err.display_chain().to_string() }
}