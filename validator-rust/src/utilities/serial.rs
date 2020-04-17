//! Serialization and deserialization between prost protobuf structs and internal representations

use crate::errors::*;

use crate::proto;
use std::collections::{HashMap};
use crate::base::{Release, Nature, Jagged, Vector1D, Value, Array, Vector1DNull, NatureCategorical, NatureContinuous, AggregatorProperties, ValueProperties, HashmapProperties, JaggedProperties, DataType, Hashmap, ArrayProperties, ReleaseNode};

// PARSERS
pub fn parse_bool_null(value: &proto::BoolNull) -> Option<bool> {
    match value.data.to_owned() {
        Some(elem_data) => match elem_data { proto::bool_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_i64_null(value: &proto::I64Null) -> Option<i64> {
    match value.data.to_owned() {
        Some(elem_data) => match elem_data { proto::i64_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_f64_null(value: &proto::F64Null) -> Option<f64> {
    match value.data.to_owned() {
        Some(elem_data) => match elem_data { proto::f64_null::Data::Option(x) => Some(x) },
        None => None
    }
}

pub fn parse_str_null(value: &proto::StrNull) -> Option<String> {
    match value.data.to_owned() {
        Some(elem_data) => match elem_data { proto::str_null::Data::Option(x) => Some(x) },
        None => None
    }
}


pub fn parse_array1d_bool_null(value: &proto::Array1dBoolNull) -> Vec<Option<bool>> {
    value.data.iter().map(parse_bool_null).collect()
}

pub fn parse_array1d_i64_null(value: &proto::Array1dI64Null) -> Vec<Option<i64>> {
    value.data.iter().map(parse_i64_null).collect()
}

pub fn parse_array1d_f64_null(value: &proto::Array1dF64Null) -> Vec<Option<f64>> {
    value.data.iter().map(parse_f64_null).collect()
}

pub fn parse_array1d_str_null(value: &proto::Array1dStrNull) -> Vec<Option<String>> {
    value.data.iter().map(parse_str_null).collect()
}

pub fn parse_array1d_null(value: &proto::Array1dNull) -> Vector1DNull {
    match value.data.to_owned().unwrap() {
        proto::array1d_null::Data::Bool(vector) => Vector1DNull::Bool(parse_array1d_bool_null(&vector)),
        proto::array1d_null::Data::I64(vector) => Vector1DNull::I64(parse_array1d_i64_null(&vector)),
        proto::array1d_null::Data::F64(vector) => Vector1DNull::F64(parse_array1d_f64_null(&vector)),
        proto::array1d_null::Data::String(vector) => Vector1DNull::Str(parse_array1d_str_null(&vector)),
    }
}


pub fn parse_array1d_bool(value: &proto::Array1dBool) -> Vec<bool> { value.data.to_owned() }

pub fn parse_array1d_i64(value: &proto::Array1dI64) -> Vec<i64> { value.data.to_owned() }

pub fn parse_array1d_f64(value: &proto::Array1dF64) -> Vec<f64> { value.data.to_owned() }

pub fn parse_array1d_str(value: &proto::Array1dStr) -> Vec<String> { value.data.to_owned() }


pub fn parse_array1d(value: &proto::Array1d) -> Vector1D {
    match value.data.to_owned().unwrap() {
        proto::array1d::Data::Bool(vector) => Vector1D::Bool(parse_array1d_bool(&vector)),
        proto::array1d::Data::I64(vector) => Vector1D::I64(parse_array1d_i64(&vector)),
        proto::array1d::Data::F64(vector) => Vector1D::F64(parse_array1d_f64(&vector)),
        proto::array1d::Data::String(vector) => Vector1D::Str(parse_array1d_str(&vector)),
    }
}


pub fn parse_arraynd(value: &proto::ArrayNd) -> Array {
    let shape: Vec<usize> = value.shape.iter().map(|x| *x as usize).collect();
    match parse_array1d(&value.flattened.to_owned().unwrap()) {
        Vector1D::Bool(vector) => Array::Bool(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::I64(vector) => Array::I64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::F64(vector) => Array::F64(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Str(vector) => Array::Str(ndarray::Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
    }
}

pub fn parse_hashmap_str(value: &proto::HashmapStr) -> HashMap<String, Value> {
    value.data.iter().map(|(name, data)| (name.clone(), parse_value(data).unwrap())).collect()
}
pub fn parse_hashmap_i64(value: &proto::HashmapI64) -> HashMap<i64, Value> {
    value.data.iter().map(|(name, data)| (*name, parse_value(data).unwrap())).collect()
}
pub fn parse_hashmap_bool(value: &proto::HashmapBool) -> HashMap<bool, Value> {
    value.data.iter().map(|(name, data)| (*name, parse_value(data).unwrap())).collect()
}

pub fn parse_hashmap(value: &proto::Hashmap) -> Hashmap<Value> {
    match value.variant.clone().unwrap() {
        proto::hashmap::Variant::String(value) => Hashmap::Str(parse_hashmap_str(&value)),
        proto::hashmap::Variant::I64(value) => Hashmap::I64(parse_hashmap_i64(&value)),
        proto::hashmap::Variant::Bool(value) => Hashmap::Bool(parse_hashmap_bool(&value)),
    }
}

pub fn parse_array1d_option(value: &proto::Array1dOption) -> Option<Vector1D> {
    match value.data.to_owned() {
        Some(data) => match data {
            proto::array1d_option::Data::Option(data) => Some(parse_array1d(&data)),
        },
        None => None
    }
}

pub fn parse_data_type(value: proto::DataType) -> DataType {
    match value {
        proto::DataType::Bool => DataType::Bool,
        proto::DataType::F64 => DataType::F64,
        proto::DataType::I64 => DataType::I64,
        proto::DataType::String => DataType::Str,
    }
}

pub fn parse_array2d_jagged(value: &proto::Array2dJagged) -> Jagged {
    match proto::DataType::from_i32(value.data_type).unwrap() {
        proto::DataType::Bool => Jagged::Bool(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::Bool(vector) => vector,
                    _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<bool>>>>()),
        proto::DataType::F64 => Jagged::F64(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::F64(vector) => vector,
                    _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<f64>>>>()),
        proto::DataType::I64 => Jagged::I64(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::I64(vector) => vector,
                    _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<i64>>>>()),
        proto::DataType::String => Jagged::Str(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::Str(vector) => vector,
                    _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<String>>>>()),
    }
}

pub fn parse_value(value: &proto::Value) -> Result<Value> {
    Ok(match value.data.to_owned().unwrap() {
        proto::value::Data::Array(data) =>
            Value::Array(parse_arraynd(&data)),
        proto::value::Data::Hashmap(data) =>
            Value::Hashmap(parse_hashmap(&data)),
        proto::value::Data::Jagged(data) =>
            Value::Jagged(parse_array2d_jagged(&data))
    })
}

pub fn parse_release(release: &proto::Release) -> Result<Release> {
    release.values.iter().map(|(idx, release_node)| Ok((*idx, parse_release_node(release_node)?)))
        .collect::<Result<HashMap<u32, ReleaseNode>>>()
}

pub fn parse_release_node(release_node: &proto::ReleaseNode) -> Result<ReleaseNode> {
    Ok(ReleaseNode {
        value: parse_value(release_node.value.as_ref()
            .ok_or_else(|| Error::from("value must be defined in a release node"))?)?,
        privacy_usages: release_node.privacy_usages.clone().map(|v| v.values),
        public: release_node.public
    })
}

pub fn parse_value_properties(value: &proto::ValueProperties) -> ValueProperties {
    match value.variant.clone().unwrap() {
        proto::value_properties::Variant::Hashmap(value) =>
            ValueProperties::Hashmap(parse_hashmap_properties(&value)),
        proto::value_properties::Variant::Array(value) =>
            ValueProperties::Array(parse_arraynd_properties(&value)),
        proto::value_properties::Variant::Jagged(value) =>
            ValueProperties::Jagged(parse_array2d_jagged_properties(&value)),
    }
}

pub fn parse_hashmap_properties_str(value: &proto::HashmapValuePropertiesStr) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::Str(value.data.iter()
        .map(|(name, properties)| (name.clone(), parse_value_properties(properties)))
        .collect())
}
pub fn parse_hashmap_properties_bool(value: &proto::HashmapValuePropertiesBool) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::Bool(value.data.iter()
        .map(|(name, properties)| (*name, parse_value_properties(properties)))
        .collect())
}
pub fn parse_hashmap_properties_i64(value: &proto::HashmapValuePropertiesI64) -> Hashmap<ValueProperties> {
    Hashmap::<ValueProperties>::I64(value.data.iter()
        .map(|(name, properties)| (*name, parse_value_properties(properties)))
        .collect())
}

pub fn parse_hashmap_properties(value: &proto::HashmapProperties) -> HashmapProperties {
    HashmapProperties {
        num_records: parse_i64_null(&value.num_records.clone().unwrap()),
        disjoint: false,
        properties: match value.value_properties.clone().unwrap().variant.unwrap() {
            proto::hashmap_value_properties::Variant::String(value) => parse_hashmap_properties_str(&value),
            proto::hashmap_value_properties::Variant::Bool(value) => parse_hashmap_properties_bool(&value),
            proto::hashmap_value_properties::Variant::I64(value) => parse_hashmap_properties_i64(&value),
        },
        columnar: value.columnar
    }
}

pub fn parse_arraynd_properties(value: &proto::ArrayNdProperties) -> ArrayProperties {
    ArrayProperties {
        num_records: parse_i64_null(&value.num_records.to_owned().unwrap()),
        num_columns: parse_i64_null(&value.num_columns.to_owned().unwrap()),
        nullity: value.nullity,
        releasable: value.releasable,
        c_stability: parse_array1d_f64(&value.c_stability.to_owned().unwrap()),
        aggregator: match value.aggregator.clone() {
            Some(aggregator) => Some(AggregatorProperties {
                component: aggregator.component.clone().unwrap().variant.unwrap(),
                properties: aggregator.properties.iter()
                    .map(|(name, properties)| (name.clone(), parse_value_properties(&properties)))
                    .collect::<HashMap<String, ValueProperties>>()
            }),
            None => None
        },
        nature: match value.nature.to_owned() {
            Some(nature) => match nature {
                proto::array_nd_properties::Nature::Continuous(continuous) =>
                    Some(Nature::Continuous(NatureContinuous {
                        lower: parse_array1d_null(&continuous.minimum.unwrap()),
                        upper: parse_array1d_null(&continuous.maximum.unwrap()),
                    })),
                proto::array_nd_properties::Nature::Categorical(categorical) =>
                    Some(Nature::Categorical(NatureCategorical {
                        categories: parse_array2d_jagged(&categorical.categories.unwrap())
                    }))
            },
            None => None,
        },
        data_type: parse_data_type(proto::DataType::from_i32(value.data_type).unwrap()),
        dataset_id: value.dataset_id.as_ref().and_then(parse_i64_null)
    }
}

pub fn parse_array2d_jagged_properties(value: &proto::Vector2DJaggedProperties) -> JaggedProperties {
    JaggedProperties {
        releasable: value.releasable
    }
}


// SERIALIZERS
pub fn serialize_bool_null(value: &Option<bool>) -> proto::BoolNull {
    proto::BoolNull {
        data: match value {
            Some(elem_data) => Some(proto::bool_null::Data::Option(*elem_data)),
            None => None
        }
    }
}

pub fn serialize_i64_null(value: &Option<i64>) -> proto::I64Null {
    proto::I64Null {
        data: match value {
            Some(elem_data) => Some(proto::i64_null::Data::Option(*elem_data)),
            None => None
        }
    }
}

pub fn serialize_f64_null(value: &Option<f64>) -> proto::F64Null {
    proto::F64Null {
        data: match value {
            Some(elem_data) => Some(proto::f64_null::Data::Option(*elem_data)),
            None => None
        }
    }
}

pub fn serialize_str_null(value: &Option<String>) -> proto::StrNull {
    proto::StrNull {
        data: match value {
            Some(elem_data) => Some(proto::str_null::Data::Option(elem_data.to_owned())),
            None => None
        }
    }
}


pub fn serialize_array1d_bool_null(value: &[Option<bool>]) -> proto::Array1dBoolNull {
    proto::Array1dBoolNull {
        data: value.iter().map(serialize_bool_null).collect()
    }
}

pub fn serialize_array1d_i64_null(value: &[Option<i64>]) -> proto::Array1dI64Null {
    proto::Array1dI64Null {
        data: value.iter().map(serialize_i64_null).collect()
    }
}

pub fn serialize_array1d_f64_null(value: &[Option<f64>]) -> proto::Array1dF64Null {
    proto::Array1dF64Null {
        data: value.iter().map(serialize_f64_null).collect()
    }
}

pub fn serialize_array1d_str_null(value: &[Option<String>]) -> proto::Array1dStrNull {
    proto::Array1dStrNull {
        data: value.iter().map(serialize_str_null).collect()
    }
}


pub fn serialize_array1d_null(value: &Vector1DNull) -> proto::Array1dNull {
    proto::Array1dNull {
        data: Some(match value {
            Vector1DNull::Bool(vector) => proto::array1d_null::Data::Bool(serialize_array1d_bool_null(&vector)),
            Vector1DNull::I64(vector) => proto::array1d_null::Data::I64(serialize_array1d_i64_null(&vector)),
            Vector1DNull::F64(vector) => proto::array1d_null::Data::F64(serialize_array1d_f64_null(&vector)),
            Vector1DNull::Str(vector) => proto::array1d_null::Data::String(serialize_array1d_str_null(&vector)),
        })
    }
}


pub fn serialize_array1d_bool(value: &[bool]) -> proto::Array1dBool { proto::Array1dBool { data: value.to_owned() } }

pub fn serialize_array1d_i64(value: &[i64]) -> proto::Array1dI64 { proto::Array1dI64 { data: value.to_owned() } }

pub fn serialize_array1d_f64(value: &[f64]) -> proto::Array1dF64 { proto::Array1dF64 { data: value.to_owned() } }

pub fn serialize_array1d_str(value: &[String]) -> proto::Array1dStr { proto::Array1dStr { data: value.to_owned() } }


pub fn serialize_array1d(value: &Vector1D) -> proto::Array1d {
    proto::Array1d {
        data: Some(match value {
            Vector1D::Bool(vector) => proto::array1d::Data::Bool(serialize_array1d_bool(vector)),
            Vector1D::I64(vector) => proto::array1d::Data::I64(serialize_array1d_i64(vector)),
            Vector1D::F64(vector) => proto::array1d::Data::F64(serialize_array1d_f64(vector)),
            Vector1D::Str(vector) => proto::array1d::Data::String(serialize_array1d_str(vector)),
        })
    }
}

pub fn serialize_arraynd(value: &Array) -> proto::ArrayNd {
    match value {
        Array::Bool(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::Bool(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::F64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::F64(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::I64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::I64(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        Array::Str(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::Str(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        }
    }
}

pub fn serialize_hashmap_str(value: &HashMap<String, Value>) -> proto::HashmapStr {
    proto::HashmapStr {
        data: value.iter()
            .map(|(name, value)| (name.clone(), serialize_value(value).unwrap()))
            .collect()
    }
}
pub fn serialize_hashmap_bool(value: &HashMap<bool, Value>) -> proto::HashmapBool {
    proto::HashmapBool {
        data: value.iter()
            .map(|(name, value)| (*name, serialize_value(value).unwrap()))
            .collect()
    }
}
pub fn serialize_hashmap_i64(value: &HashMap<i64, Value>) -> proto::HashmapI64 {
    proto::HashmapI64 {
        data: value.iter()
            .map(|(name, value)| (*name, serialize_value(value).unwrap()))
            .collect()
    }
}


pub fn serialize_hashmap(value: &Hashmap<Value>) -> proto::Hashmap {
    proto::Hashmap {
        variant: Some(match value {
            Hashmap::Str(value) => proto::hashmap::Variant::String(serialize_hashmap_str(value)),
            Hashmap::Bool(value) => proto::hashmap::Variant::Bool(serialize_hashmap_bool(value)),
            Hashmap::I64(value) => proto::hashmap::Variant::I64(serialize_hashmap_i64(value))
        })
    }
}

pub fn serialize_array1d_option(value: &Option<Vector1D>) -> proto::Array1dOption {
    proto::Array1dOption {
        data: Some(proto::array1d_option::Data::Option(serialize_array1d(&value.to_owned().unwrap())))
    }
}

pub fn serialize_data_type(value: &DataType) -> proto::DataType {
    match value {
        DataType::Bool => proto::DataType::Bool,
        DataType::F64 => proto::DataType::F64,
        DataType::I64 => proto::DataType::I64,
        DataType::Str => proto::DataType::String,
    }
}

pub fn serialize_array2d_jagged(value: &Jagged) -> proto::Array2dJagged {
    proto::Array2dJagged {
        data_type: match value {
            Jagged::Bool(_x) => proto::DataType::Bool as i32,
            Jagged::F64(_x) => proto::DataType::F64 as i32,
            Jagged::I64(_x) => proto::DataType::I64 as i32,
            Jagged::Str(_x) => proto::DataType::String as i32,
        },
        data: match value {
            Jagged::Bool(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some(column) => Some(Vector1D::Bool(column.to_owned())),
                None => None
            })).collect(),
            Jagged::F64(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some(column) => Some(Vector1D::F64(column.to_owned())),
                None => None
            })).collect(),
            Jagged::I64(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some(column) => Some(Vector1D::I64(column.to_owned())),
                None => None
            })).collect(),
            Jagged::Str(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some(column) => Some(Vector1D::Str(column.to_owned())),
                None => None
            })).collect(),
        },
    }
}

pub fn serialize_value(value: &Value) -> Result<proto::Value> {
    Ok(proto::Value {
        data: Some(match value {
            Value::Array(data) =>
                proto::value::Data::Array(serialize_arraynd(data)),
            Value::Hashmap(data) =>
                proto::value::Data::Hashmap(serialize_hashmap(data)),
            Value::Jagged(data) =>
                proto::value::Data::Jagged(serialize_array2d_jagged(data))
        })
    })
}

pub fn serialize_release(release: &Release) -> Result<proto::Release> {
    Ok(proto::Release {
        values: release.into_iter()
            .map(|(idx, release_node)| Ok((*idx, serialize_release_node(release_node)?)))
            .collect::<Result<HashMap<u32, proto::ReleaseNode>>>()?
    })
}

pub fn serialize_release_node(release_node: &ReleaseNode) -> Result<proto::ReleaseNode> {
    Ok(proto::ReleaseNode {
        value: Some(serialize_value(&release_node.value)?),
        privacy_usages: release_node.privacy_usages.as_ref().map(|v| proto::PrivacyUsages {values: v.clone()}),
        public: release_node.public
    })
}

pub fn serialize_hashmap_properties_str(value: &HashMap<String, ValueProperties>) -> proto::HashmapValuePropertiesStr {
    proto::HashmapValuePropertiesStr {
        data: value.iter()
            .map(|(name, value)| (name.clone(), serialize_value_properties(value)))
            .collect::<HashMap<String, proto::ValueProperties>>()
    }
}
pub fn serialize_hashmap_properties_i64(value: &HashMap<i64, ValueProperties>) -> proto::HashmapValuePropertiesI64 {
    proto::HashmapValuePropertiesI64 {
        data: value.iter()
            .map(|(name, value)| (*name, serialize_value_properties(value)))
            .collect::<HashMap<i64, proto::ValueProperties>>()
    }
}
pub fn serialize_hashmap_properties_bool(value: &HashMap<bool, ValueProperties>) -> proto::HashmapValuePropertiesBool {
    proto::HashmapValuePropertiesBool {
        data: value.iter()
            .map(|(name, value)| (*name, serialize_value_properties(value)))
            .collect::<HashMap<bool, proto::ValueProperties>>()
    }
}

pub fn serialize_hashmap_properties(value: &HashmapProperties) -> proto::HashmapProperties {
    proto::HashmapProperties {
        num_records: Some(serialize_i64_null(&value.num_records)),
        disjoint: value.disjoint,
        value_properties: Some(proto::HashmapValueProperties {
            variant: Some(match value.properties.clone() {
                Hashmap::Str(value) => proto::hashmap_value_properties::Variant::String(serialize_hashmap_properties_str(&value)),
                Hashmap::I64(value) => proto::hashmap_value_properties::Variant::I64(serialize_hashmap_properties_i64(&value)),
                Hashmap::Bool(value) => proto::hashmap_value_properties::Variant::Bool(serialize_hashmap_properties_bool(&value)),
            })
        }),
        columnar: value.columnar
    }
}

pub fn serialize_arraynd_properties(value: &ArrayProperties) -> proto::ArrayNdProperties {
    proto::ArrayNdProperties {
        num_records: Some(serialize_i64_null(&value.num_records)),
        num_columns: Some(serialize_i64_null(&value.num_columns)),
        nullity: value.nullity,
        releasable: value.releasable,
        c_stability: Some(serialize_array1d_f64(&value.c_stability)),
        nature: match value.clone().nature {
            Some(nature) => match nature {
                Nature::Categorical(categorical) => Some(proto::array_nd_properties::Nature::Categorical(proto::NatureCategorical {
                    categories: Some(serialize_array2d_jagged(&categorical.categories))
                })),
                Nature::Continuous(x) => Some(proto::array_nd_properties::Nature::Continuous(proto::NatureContinuous {
                    minimum: Some(serialize_array1d_null(&x.lower)),
                    maximum: Some(serialize_array1d_null(&x.upper)),
                }))
            },
            None => None
        },
        aggregator: match value.aggregator.clone() {
            Some(aggregator) => Some(proto::array_nd_properties::AggregatorProperties {
                component: Some(proto::Component {
                    variant: Some(aggregator.component),
                    omit: true, batch: 0, arguments: HashMap::new(),
                }),
                properties: aggregator.properties.iter()
                    .map(|(name, properties)| (name.clone(), serialize_value_properties(&properties)))
                    .collect::<HashMap<String, proto::ValueProperties>>()
            }),
            None => None
        },
        data_type: serialize_data_type(&value.data_type) as i32,
        dataset_id: Some(serialize_i64_null(&value.dataset_id))
    }
}

pub fn serialize_vector2d_jagged_properties(value: &JaggedProperties) -> proto::Vector2DJaggedProperties {
    proto::Vector2DJaggedProperties {
        releasable: value.releasable
    }
}

pub fn serialize_value_properties(value: &ValueProperties) -> proto::ValueProperties {
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
