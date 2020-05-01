//! Serialization and deserialization between prost protobuf structs and internal representations

use crate::proto;
use std::collections::HashMap;
use crate::base::{Release, Nature, Jagged, Vector1D, Value, Array, Vector1DNull, NatureCategorical, NatureContinuous, AggregatorProperties, ValueProperties, HashmapProperties, JaggedProperties, DataType, Hashmap, ArrayProperties, ReleaseNode};
use indexmap::IndexMap;

// PARSERS
pub fn parse_bool_null(value: proto::BoolNull) -> Option<bool> {
    match value.data {
        Some(elem_data) => match elem_data { proto::bool_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_i64_null(value: proto::I64Null) -> Option<i64> {
    match value.data {
        Some(elem_data) => match elem_data { proto::i64_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_f64_null(value: proto::F64Null) -> Option<f64> {
    match value.data {
        Some(elem_data) => match elem_data { proto::f64_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_str_null(value: proto::StrNull) -> Option<String> {
    match value.data {
        Some(elem_data) => match elem_data { proto::str_null::Data::Option(x) => Some(x) },
        None => None
    }
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


pub fn parse_arraynd(value: proto::ArrayNd) -> Array {
    let shape: Vec<usize> = value.shape.into_iter().map(|x| x as usize).collect();
    match parse_array1d(value.flattened.unwrap()) {
        Vector1D::Bool(vector) => Array::Bool(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::I64(vector) => Array::I64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::F64(vector) => Array::F64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Str(vector) => Array::Str(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
    }
}

pub fn parse_hashmap_str(value: proto::HashmapStr) -> IndexMap<String, Value> {
    value.data.into_iter().map(|(name, data)| (name, parse_value(data))).collect()
}

pub fn parse_hashmap_i64(value: proto::HashmapI64) -> IndexMap<i64, Value> {
    value.data.into_iter().map(|(name, data)| (name, parse_value(data))).collect()
}

pub fn parse_hashmap_bool(value: proto::HashmapBool) -> IndexMap<bool, Value> {
    value.data.into_iter().map(|(name, data)| (name, parse_value(data))).collect()
}

pub fn parse_hashmap(value: proto::Hashmap) -> Hashmap<Value> {
    match value.variant.clone().unwrap() {
        proto::hashmap::Variant::String(value) => Hashmap::Str(parse_hashmap_str(value)),
        proto::hashmap::Variant::I64(value) => Hashmap::I64(parse_hashmap_i64(value)),
        proto::hashmap::Variant::Bool(value) => Hashmap::Bool(parse_hashmap_bool(value)),
    }
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

pub fn parse_array2d_jagged(value: proto::Array2dJagged) -> Jagged {
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
            Value::Array(parse_arraynd(data)),
        proto::value::Data::Hashmap(data) =>
            Value::Hashmap(parse_hashmap(data)),
        proto::value::Data::Jagged(data) =>
            Value::Jagged(parse_array2d_jagged(data))
    }
}

pub fn parse_release(release: proto::Release) -> Release {
    release.values.into_iter().map(|(idx, release_node)| (idx, parse_release_node(release_node)))
        .collect::<HashMap<u32, ReleaseNode>>()
}

pub fn parse_release_node(release_node: proto::ReleaseNode) -> ReleaseNode {
    ReleaseNode {
        value: parse_value(release_node.value.unwrap()),
        privacy_usages: release_node.privacy_usages.clone().map(|v| v.values),
        public: release_node.public,
    }
}

pub fn parse_value_properties(value: proto::ValueProperties) -> ValueProperties {
    match value.variant.clone().unwrap() {
        proto::value_properties::Variant::Hashmap(value) =>
            ValueProperties::Hashmap(parse_hashmap_properties(value)),
        proto::value_properties::Variant::Array(value) =>
            ValueProperties::Array(parse_arraynd_properties(value)),
        proto::value_properties::Variant::Jagged(value) =>
            ValueProperties::Jagged(parse_array2d_jagged_properties(value)),
    }
}

pub fn parse_hashmap_properties_str(value: proto::HashmapValuePropertiesStr) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::Str(value.data.into_iter()
        .map(|(name, properties)| (name, parse_value_properties(properties)))
        .collect())
}

pub fn parse_hashmap_properties_bool(value: proto::HashmapValuePropertiesBool) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::Bool(value.data.into_iter()
        .map(|(name, properties)| (name, parse_value_properties(properties)))
        .collect())
}

pub fn parse_hashmap_properties_i64(value: proto::HashmapValuePropertiesI64) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::I64(value.data.into_iter()
        .map(|(name, properties)| (name, parse_value_properties(properties)))
        .collect())
}

pub fn parse_hashmap_properties(value: proto::HashmapProperties) -> HashmapProperties {
    HashmapProperties {
        num_records: value.num_records.and_then(parse_i64_null),
        disjoint: false,
        properties: match value.value_properties.clone().unwrap().variant.unwrap() {
            proto::hashmap_value_properties::Variant::String(value) => parse_hashmap_properties_str(value),
            proto::hashmap_value_properties::Variant::Bool(value) => parse_hashmap_properties_bool(value),
            proto::hashmap_value_properties::Variant::I64(value) => parse_hashmap_properties_i64(value),
        },
        variant: proto::hashmap_properties::Variant::from_i32(value.variant).unwrap(),
    }
}

pub fn parse_arraynd_properties(value: proto::ArrayNdProperties) -> ArrayProperties {
    ArrayProperties {
        num_records: value.num_records.and_then(parse_i64_null),
        num_columns: value.num_columns.and_then(parse_i64_null),
        nullity: value.nullity,
        releasable: value.releasable,
        c_stability: parse_array1d_f64(value.c_stability.to_owned().unwrap()),
        aggregator: match value.aggregator.clone() {
            Some(aggregator) => Some(AggregatorProperties {
                component: aggregator.component.unwrap().variant.unwrap(),
                properties: aggregator.properties.into_iter()
                    .map(|(name, properties)| (name, parse_value_properties(properties)))
                    .collect::<HashMap<String, ValueProperties>>(),
            }),
            None => None
        },
        nature: match value.nature.to_owned() {
            Some(nature) => match nature {
                proto::array_nd_properties::Nature::Continuous(continuous) =>
                    Some(Nature::Continuous(NatureContinuous {
                        lower: parse_array1d_null(continuous.minimum.unwrap()),
                        upper: parse_array1d_null(continuous.maximum.unwrap()),
                    })),
                proto::array_nd_properties::Nature::Categorical(categorical) =>
                    Some(Nature::Categorical(NatureCategorical {
                        categories: parse_array2d_jagged(categorical.categories.unwrap())
                    }))
            },
            None => None,
        },
        data_type: parse_data_type(proto::DataType::from_i32(value.data_type).unwrap()),
        dataset_id: value.dataset_id.and_then(parse_i64_null),
        is_not_empty: value.is_not_empty,
        dimensionality: value.dimensionality.and_then(parse_i64_null),
    }
}

pub fn parse_array2d_jagged_properties(value: proto::Vector2DJaggedProperties) -> JaggedProperties {
    JaggedProperties {
        releasable: value.releasable
    }
}


// SERIALIZERS
pub fn serialize_bool_null(value: Option<bool>) -> proto::BoolNull {
    proto::BoolNull {
        data: match value {
            Some(elem_data) => Some(proto::bool_null::Data::Option(elem_data)),
            None => None
        }
    }
}

pub fn serialize_i64_null(value: Option<i64>) -> proto::I64Null {
    proto::I64Null {
        data: match value {
            Some(elem_data) => Some(proto::i64_null::Data::Option(elem_data)),
            None => None
        }
    }
}

pub fn serialize_f64_null(value: Option<f64>) -> proto::F64Null {
    proto::F64Null {
        data: match value {
            Some(elem_data) => Some(proto::f64_null::Data::Option(elem_data)),
            None => None
        }
    }
}

pub fn serialize_str_null(value: Option<String>) -> proto::StrNull {
    proto::StrNull {
        data: match value {
            Some(elem_data) => Some(proto::str_null::Data::Option(elem_data)),
            None => None
        }
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

pub fn serialize_arraynd(value: Array) -> proto::ArrayNd {
    match value {
        Array::Bool(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(Vector1D::Bool(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::F64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(Vector1D::F64(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::I64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(Vector1D::I64(array.iter().map(|s| *s).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Str(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(Vector1D::Str(array.iter().map(|s| s.to_owned()).collect()))),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        }
    }
}

pub fn serialize_hashmap_str(value: IndexMap<String, Value>) -> proto::HashmapStr {
    proto::HashmapStr {
        data: value.into_iter()
            .map(|(name, value)| (name, serialize_value(value)))
            .collect()
    }
}

pub fn serialize_hashmap_bool(value: IndexMap<bool, Value>) -> proto::HashmapBool {
    proto::HashmapBool {
        data: value.into_iter()
            .map(|(name, value)| (name, serialize_value(value)))
            .collect()
    }
}

pub fn serialize_hashmap_i64(value: IndexMap<i64, Value>) -> proto::HashmapI64 {
    proto::HashmapI64 {
        data: value.into_iter()
            .map(|(name, value)| (name, serialize_value(value)))
            .collect()
    }
}


pub fn serialize_hashmap(value: Hashmap<Value>) -> proto::Hashmap {
    proto::Hashmap {
        variant: Some(match value {
            Hashmap::Str(value) => proto::hashmap::Variant::String(serialize_hashmap_str(value)),
            Hashmap::Bool(value) => proto::hashmap::Variant::Bool(serialize_hashmap_bool(value)),
            Hashmap::I64(value) => proto::hashmap::Variant::I64(serialize_hashmap_i64(value))
        })
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

pub fn serialize_array2d_jagged(value: Jagged) -> proto::Array2dJagged {
    proto::Array2dJagged {
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
                proto::value::Data::Array(serialize_arraynd(data)),
            Value::Hashmap(data) =>
                proto::value::Data::Hashmap(serialize_hashmap(data)),
            Value::Jagged(data) =>
                proto::value::Data::Jagged(serialize_array2d_jagged(data))
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
        public: release_node.public,
    }
}

pub fn serialize_hashmap_properties_str(value: IndexMap<String, ValueProperties>) -> proto::HashmapValuePropertiesStr {
    proto::HashmapValuePropertiesStr {
        data: value.into_iter()
            .map(|(name, value)| (name.clone(), serialize_value_properties(value)))
            .collect::<HashMap<String, proto::ValueProperties>>()
    }
}

pub fn serialize_hashmap_properties_i64(value: IndexMap<i64, ValueProperties>) -> proto::HashmapValuePropertiesI64 {
    proto::HashmapValuePropertiesI64 {
        data: value.into_iter()
            .map(|(name, value)| (name, serialize_value_properties(value)))
            .collect::<HashMap<i64, proto::ValueProperties>>()
    }
}

pub fn serialize_hashmap_properties_bool(value: IndexMap<bool, ValueProperties>) -> proto::HashmapValuePropertiesBool {
    proto::HashmapValuePropertiesBool {
        data: value.into_iter()
            .map(|(name, value)| (name, serialize_value_properties(value)))
            .collect::<HashMap<bool, proto::ValueProperties>>()
    }
}

pub fn serialize_hashmap_properties(value: HashmapProperties) -> proto::HashmapProperties {
    proto::HashmapProperties {
        num_records: Some(serialize_i64_null(value.num_records)),
        disjoint: value.disjoint,
        value_properties: Some(proto::HashmapValueProperties {
            variant: Some(match value.properties {
                Hashmap::Str(value) => proto::hashmap_value_properties::Variant::String(serialize_hashmap_properties_str(value)),
                Hashmap::I64(value) => proto::hashmap_value_properties::Variant::I64(serialize_hashmap_properties_i64(value)),
                Hashmap::Bool(value) => proto::hashmap_value_properties::Variant::Bool(serialize_hashmap_properties_bool(value)),
            })
        }),
        variant: value.variant as i32
    }
}

pub fn serialize_arraynd_properties(value: ArrayProperties) -> proto::ArrayNdProperties {

    let ArrayProperties {
        num_records, num_columns, nullity, releasable, c_stability, aggregator, nature, data_type, dataset_id, is_not_empty, dimensionality
    } = value;

    proto::ArrayNdProperties {
        num_records: Some(serialize_i64_null(num_records)),
        num_columns: Some(serialize_i64_null(num_columns)),
        nullity,
        releasable,
        c_stability: Some(serialize_array1d_f64(c_stability)),
        nature: match nature {
            Some(nature) => match nature {
                Nature::Categorical(categorical) => Some(proto::array_nd_properties::Nature::Categorical(proto::NatureCategorical {
                    categories: Some(serialize_array2d_jagged(categorical.categories))
                })),
                Nature::Continuous(x) => Some(proto::array_nd_properties::Nature::Continuous(proto::NatureContinuous {
                    minimum: Some(serialize_array1d_null(x.lower)),
                    maximum: Some(serialize_array1d_null(x.upper)),
                }))
            },
            None => None
        },
        aggregator: match aggregator.clone() {
            Some(aggregator) => Some(proto::array_nd_properties::AggregatorProperties {
                component: Some(proto::Component {
                    variant: Some(aggregator.component),
                    omit: true,
                    batch: 0,
                    arguments: HashMap::new(),
                }),
                properties: aggregator.properties.into_iter()
                    .map(|(name, properties)| (name, serialize_value_properties(properties)))
                    .collect::<HashMap<String, proto::ValueProperties>>(),
            }),
            None => None
        },
        data_type: serialize_data_type(data_type) as i32,
        dataset_id: Some(serialize_i64_null(dataset_id)),
        is_not_empty,
        dimensionality: Some(serialize_i64_null(dimensionality)),
    }
}

pub fn serialize_vector2d_jagged_properties(value: JaggedProperties) -> proto::Vector2DJaggedProperties {
    proto::Vector2DJaggedProperties {
        releasable: value.releasable
    }
}

pub fn serialize_value_properties(value: ValueProperties) -> proto::ValueProperties {
    proto::ValueProperties {
        variant: Some(match value {
            ValueProperties::Hashmap(value) =>
                proto::value_properties::Variant::Hashmap(serialize_hashmap_properties(value)),
            ValueProperties::Array(value) =>
                proto::value_properties::Variant::Array(serialize_arraynd_properties(value)),
            ValueProperties::Jagged(value) =>
                proto::value_properties::Variant::Jagged(serialize_vector2d_jagged_properties(value))
        })
    }
}
