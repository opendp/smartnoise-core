use ndarray::prelude::*;
use crate::proto;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Debug)]
pub enum Vector1DNull {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

#[derive(Clone, Debug)]
pub enum Vector1D {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

#[derive(Clone, Debug)]
pub enum ArrayND {
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
}

// used for categorical constraints
#[derive(Clone, Debug)]
pub enum Vector2DJagged {
    Bool(Vec<Option<Vec<bool>>>),
    I64(Vec<Option<Vec<i64>>>),
    F64(Vec<Option<Vec<f64>>>),
    Str(Vec<Option<Vec<String>>>),
}

// used exclusively in the runtime for node evaluation
#[derive(Clone, Debug)]
pub enum Value {
    ArrayND(ArrayND),
    HashmapString(HashMap<String, Value>),
    Vector2DJagged(Vector2DJagged),
}

// PARSERS
pub fn parse_bool_null(value: &proto::BoolNull) -> Option<bool> {
//    match value { proto::bool_null::Data::Option(x) => Some(x), _ => None }
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


pub fn parse_arrayNd(value: &proto::ArrayNd) -> ArrayND {
    let shape: Vec<usize> = value.shape.iter().map(|x| *x as usize).collect();
    match parse_array1d(&value.flattened.to_owned().unwrap()) {
        Vector1D::Bool(vector) => ArrayND::Bool(Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::I64(vector) => ArrayND::I64(Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::F64(vector) => ArrayND::F64(Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
        Vector1D::Str(vector) => ArrayND::Str(Array::from_shape_vec(shape, vector).unwrap().into_dyn()),
    }
}

pub fn parse_hashmap_str(value: &proto::HashmapString) -> HashMap<String, Value> {
    value.data.iter().map(|(name, data)| (name.clone(), parse_value(data).unwrap())).collect()
}

pub fn parse_array1d_option(value: &proto::Array1dOption) -> Option<Vector1D> {
    match value.data.to_owned() {
        Some(data) => match data {
            proto::array1d_option::Data::Option(data) => Some(parse_array1d(&data)),
        },
        None => None
    }
}

pub fn parse_array2d_jagged(value: &proto::Array2dJagged) -> Vector2DJagged {
    match proto::array2d_jagged::DataType::from_i32(value.data_type).unwrap() {
        proto::array2d_jagged::DataType::Bool => Vector2DJagged::Bool(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::Bool(vector) => vector, _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<bool>>>>()),
        proto::array2d_jagged::DataType::F64 => Vector2DJagged::F64(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::F64(vector) => vector, _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<f64>>>>()),
        proto::array2d_jagged::DataType::I64 => Vector2DJagged::I64(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::I64(vector) => vector, _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<i64>>>>()),
        proto::array2d_jagged::DataType::String => Vector2DJagged::Str(value.data.iter()
            .map(|column| match parse_array1d_option(column) {
                Some(vector) => Some(match vector {
                    Vector1D::Str(vector) => vector, _ => panic!()
                }),
                None => None
            }).collect::<Vec<Option<Vec<String>>>>()),
    }
}

pub fn parse_value(value: &proto::Value) -> Result<Value, String> {
    Ok(match value.data.to_owned().unwrap() {
        proto::value::Data::ArrayNd(data) =>
            Value::ArrayND(parse_arrayNd(&data)),
        proto::value::Data::HashmapString(data) =>
            Value::HashmapString(parse_hashmap_str(&data)),
        proto::value::Data::Array2dJagged(data) =>
            Value::Vector2DJagged(parse_array2d_jagged(&data))
    })
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


pub fn serialize_array1d_bool_null(value: &Vec<Option<bool>>) -> proto::Array1dBoolNull {
    proto::Array1dBoolNull {
        data: value.iter().map(serialize_bool_null).collect()
    }
}

pub fn serialize_array1d_i64_null(value: &Vec<Option<i64>>) -> proto::Array1dI64Null {
    proto::Array1dI64Null {
        data: value.iter().map(serialize_i64_null).collect()
    }
}

pub fn serialize_array1d_f64_null(value: &Vec<Option<f64>>) -> proto::Array1dF64Null {
    proto::Array1dF64Null {
        data: value.iter().map(serialize_f64_null).collect()
    }
}

pub fn serialize_array1d_str_null(value: &Vec<Option<String>>) -> proto::Array1dStrNull {
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


pub fn serialize_array1d_bool(value: &Vec<bool>) -> proto::Array1dBool { proto::Array1dBool { data: value.to_owned() } }

pub fn serialize_array1d_i64(value: &Vec<i64>) -> proto::Array1dI64 { proto::Array1dI64 { data: value.to_owned() } }

pub fn serialize_array1d_f64(value: &Vec<f64>) -> proto::Array1dF64 { proto::Array1dF64 { data: value.to_owned() } }

pub fn serialize_array1d_str(value: &Vec<String>) -> proto::Array1dStr { proto::Array1dStr { data: value.to_owned() } }


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

pub fn serialize_arrayNd(value: &ArrayND) -> proto::ArrayNd {
    match value {
        ArrayND::Bool(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::Bool(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        ArrayND::F64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::F64(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        ArrayND::I64(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::I64(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        },
        ArrayND::Str(array) => proto::ArrayNd {
            flattened: Some(serialize_array1d(&Vector1D::Str(array.iter().map(|s| s.to_owned()).collect()))),
            order: (1..array.ndim()).map(|x| { x as u64 }).collect(),
            shape: array.shape().iter().map(|y| { *y as u64 }).collect(),
        }
    }
}

pub fn serialize_hashmap_str(value: &HashMap<String, Value>) -> proto::HashmapString {
    proto::HashmapString {
        data: value.iter()
            .map(|(name, value)| (name.clone(), serialize_value(value).unwrap()))
            .collect()
    }
}

pub fn serialize_array1d_option(value: &Option<Vector1D>) -> proto::Array1dOption {
    proto::Array1dOption {
        data: Some(proto::array1d_option::Data::Option(serialize_array1d(&value.to_owned().unwrap())))
    }
}

pub fn serialize_array2d_jagged(value: &Vector2DJagged) -> proto::Array2dJagged {
    proto::Array2dJagged {
        data_type: match value {
            Vector2DJagged::Bool(_x) => proto::array2d_jagged::DataType::Bool as i32,
            Vector2DJagged::F64(_x) => proto::array2d_jagged::DataType::F64 as i32,
            Vector2DJagged::I64(_x) => proto::array2d_jagged::DataType::I64 as i32,
            Vector2DJagged::Str(_x) => proto::array2d_jagged::DataType::String as i32,
        },
        data: match value {
            Vector2DJagged::Bool(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some( column) => Some(Vector1D::Bool(column.to_owned())),
                None => None
            })).collect(),
            Vector2DJagged::F64(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some( column) => Some(Vector1D::F64(column.to_owned())),
                None => None
            })).collect(),
            Vector2DJagged::I64(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some( column) => Some(Vector1D::I64(column.to_owned())),
                None => None
            })).collect(),
            Vector2DJagged::Str(data) => data.iter().map(|column| serialize_array1d_option(&match column {
                Some( column) => Some(Vector1D::Str(column.to_owned())),
                None => None
            })).collect(),
        }
    }
}

pub fn serialize_value(value: &Value) -> Result<proto::Value, String> {
    Ok(proto::Value {
        data: Some(match value {
            Value::ArrayND(data) =>
                proto::value::Data::ArrayNd(serialize_arrayNd(data)),
            Value::HashmapString(data) =>
                proto::value::Data::HashmapString(serialize_hashmap_str(data)),
            Value::Vector2DJagged(data) =>
                proto::value::Data::Array2dJagged(serialize_array2d_jagged(data))
        })
    })
}