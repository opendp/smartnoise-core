use crate::errors::*;


use ndarray::{Axis};
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use std::cmp::Ordering;
use crate::base::{ArrayND, Value, Vector2DJagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayNDProperties, DataType, HashmapProperties, Vector2DJaggedProperties, Hashmap};
use crate::utilities::serial::parse_data_type;
use std::collections::HashMap;


pub fn get_shape(array: &ArrayND) -> Vec<i64> {
    match array {
        ArrayND::Bool(array) => array.shape().to_owned(),
        ArrayND::F64(array) => array.shape().to_owned(),
        ArrayND::I64(array) => array.shape().to_owned(),
        ArrayND::Str(array) => array.shape().to_owned()
    }.iter().map(|arr| arr.clone() as i64).collect()
}

pub fn infer_num_columns(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::ArrayND(array) => {
            let shape = get_shape(&array);
            match shape.len() {
                0 => Ok(Some(1)),
                1 => Ok(Some(shape[0])),
                2 => Ok(Some(shape[1])),
                _ => Err("arrays may have max dimensionality of 2".into())
            }
        },
        Value::Hashmap(hashmap) => bail!("cannot infer number of columns on a hashmap"),
        Value::Vector2DJagged(vector) => Ok(Some(match vector {
            Vector2DJagged::Bool(vector) => vector.len(),
            Vector2DJagged::F64(vector) => vector.len(),
            Vector2DJagged::I64(vector) => vector.len(),
            Vector2DJagged::Str(vector) => vector.len(),
        } as i64))
    }
}
pub fn infer_num_rows(value: &ArrayND) -> Result<Option<i64>> {
    let shape = get_shape(value.into());
    match shape.len() {
        0 => Ok(Some(1)),
        1 | 2 => Ok(Some(shape[0])),
        _ => Err("arrays may have max dimensionality of 2".into())
    }
//
//    match value {
//        Value::ArrayND(array) => {
//            *snip*
//        },
//        Value::Hashmap(hashmap) => hashmap.values().map(|value| match value {
//            Value::ArrayND(array) => {
//                let shape = get_shape(&array);
//                match shape.len() {
//                    0 | 1 => Ok(Some(1)),
//                    2 => Ok(Some(shape[0])),
//                    _ => Err("arrays may have max dimensionality of 2".into())
//                }
//            },
//            _ => Err("properties on hashmaps are only implemented for single-column arrays".into())
//        }).collect(),
//        Value::Vector2DJagged(jagged) => Ok(match jagged {
//            Vector2DJagged::Bool(vector) => vector.iter()
//                .map(|col| match col {
//                    Some(vec) => Some(vec.len() as i64),
//                    None => None
//                }).collect(),
//            Vector2DJagged::F64(vector) => vector.iter()
//                .map(|col| match col {
//                    Some(vec) => Some(vec.len() as i64),
//                    None => None
//                }).collect(),
//            Vector2DJagged::I64(vector) => vector.iter()
//                .map(|col| match col {
//                    Some(vec) => Some(vec.len() as i64),
//                    None => None
//                }).collect(),
//            Vector2DJagged::Str(vector) => vector.iter()
//                .map(|col| match col {
//                    Some(vec) => Some(vec.len() as i64),
//                    None => None
//                }).collect(),
//        })
//        Value::Vector2DJagged(jagged) => bail!("num_rows for jagged vectors is disabled")
//    }
}

pub fn infer_min(value: &Value) -> Vec<Option<f64>> {
    match value {
        Value::ArrayND(array) => {

            match get_shape(&array).len() as i64 {
                0 => vec![Some(match array {
                    ArrayND::F64(array) =>
                        array.first().unwrap().to_owned(),
                    ArrayND::I64(array) =>
                        array.first().unwrap().to_owned() as f64,
                    _ => panic!("Cannot infer numeric min on a non-numeric vector".to_string())
                })],
                1 => match array {
                    ArrayND::F64(array) =>
                        array.iter().map(|v| Some(*v)).collect(),
                    ArrayND::I64(array) =>
                        array.iter().map(|v| Some(*v as f64)).collect(),
                    _ => panic!("Cannot infer numeric min on a non-numeric vector".to_string())
                },
                2 => match array {
                    ArrayND::F64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(col.max().unwrap().clone())).collect(),
                    ArrayND::I64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(*col.max().unwrap() as f64)).collect(),
                    _ => panic!("Cannot infer numeric min on a non-numeric vector".to_string())
                },
                _ => panic!("arrays may have max dimensionality of 2")
            }
        },
        Value::Hashmap(hashmap) => {
            let mut bound: Vec<Option<f64>> = vec![];
//            hashmap.values()
//                .map(infer_min)
//                .for_each(|next| bound.extend(next));
            bound
        }
        Value::Vector2DJagged(jagged) => {
            match jagged {
                Vector2DJagged::F64(jagged) => jagged.iter().map(|col| match col {
                    Some(col) => Some(col.iter().map(|v| v.clone()).fold1(|l, r| l.min(r)).unwrap().clone()),
                    None => None
                }).collect(),
                Vector2DJagged::I64(jagged) => jagged.iter().map(|col| match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::min).unwrap() as f64),
                    None => None
                }).collect(),
                _ => panic!("Cannot infer numeric min on a non-numeric vector")
            }
        }
    }
}
pub fn infer_max(value: &Value) -> Vec<Option<f64>> {
    match value {
        Value::ArrayND(array) => {

            match get_shape(&array).len() as i64 {
                0 => vec![Some(match array {
                    ArrayND::F64(array) =>
                        array.first().unwrap().to_owned(),
                    ArrayND::I64(array) =>
                        array.first().unwrap().to_owned() as f64,
                    _ => panic!("Cannot infer numeric max on a non-numeric vector")
                })],
                1 => match array {
                    ArrayND::F64(array) =>
                        array.iter().map(|v| Some(*v)).collect(),
                    ArrayND::I64(array) =>
                        array.iter().map(|v| Some(*v as f64)).collect(),
                    _ => panic!("Cannot infer numeric max on a non-numeric vector")
                },
                2 => match array {
                    ArrayND::F64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(col.max().unwrap().clone())).collect(),
                    ArrayND::I64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(*col.max().unwrap() as f64)).collect(),
                    _ => panic!("Cannot infer numeric max on a non-numeric vector")
                },
                _ => panic!("arrays may have max dimensionality of 2")
            }
        },
        Value::Hashmap(hashmap) => panic!("max inference is not compatible with a hashmap"),
        Value::Vector2DJagged(jagged) => {
            match jagged {
                Vector2DJagged::F64(jagged) => jagged.iter().map(|col| match col {
                    Some(col) => Some(col.iter().map(|x| x.clone()).fold1(|l, r| l.max(r).clone()).unwrap()),
                    None => None
                }).collect(),
                Vector2DJagged::I64(jagged) => jagged.iter().map(|col| match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::max).unwrap() as f64),
                    None => None
                }).collect(),
                _ => panic!("Cannot infer numeric max on a non-numeric vector")
            }
        }
    }
}


pub fn infer_categories(value: &Value) -> Vector2DJagged {
    match value {
        Value::ArrayND(array) => match array {
            ArrayND::Bool(array) =>
                Vector2DJagged::Bool(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
                    column_categories.sort();
                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::F64(array) =>
                Vector2DJagged::F64(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
                    column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::I64(array) =>
                Vector2DJagged::I64(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
                    column_categories.sort();
                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::Str(array) =>
                Vector2DJagged::Str(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
                    column_categories.sort();
                    column_categories.dedup();
                    Some(column_categories)
                }).collect())
        },
        Value::Hashmap(_hashmap) => panic!("category inference is not implemented for hashmaps"),
        Value::Vector2DJagged(jagged) => match jagged {
            Vector2DJagged::Bool(array) =>
                Vector2DJagged::Bool(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => {
                        let mut column_categories = column_categories.to_owned();
                        column_categories.sort();
                        column_categories.dedup();
                        Some(column_categories)
                    },
                    None => None
                }).collect()),
            Vector2DJagged::F64(array) =>
                Vector2DJagged::F64(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => {
                        let mut column_categories = column_categories.to_owned();
                        column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                        column_categories.dedup();
                        Some(column_categories)
                    },
                    None => None
                }).collect()),
            Vector2DJagged::I64(array) =>
                Vector2DJagged::I64(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => {
                        let mut column_categories = column_categories.to_owned();
                        column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                        column_categories.dedup();
                        Some(column_categories)
                    },
                    None => None
                }).collect()),
            Vector2DJagged::Str(array) =>
                Vector2DJagged::Str(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => {
                        let mut column_categories = column_categories.to_owned();
                        column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                        column_categories.dedup();
                        Some(column_categories)
                    },
                    None => None
                }).collect()),
        }
    }
}

pub fn infer_nature(value: &Value) -> Nature {
    match value {
        Value::ArrayND(array) => match array {
            ArrayND::F64(array) => Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::ArrayND(ArrayND::F64(array.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::ArrayND(ArrayND::F64(array.clone())))),
            }),
            ArrayND::I64(array) => Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::ArrayND(ArrayND::I64(array.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::ArrayND(ArrayND::I64(array.clone())))),
            }),
            ArrayND::Bool(array) => Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::ArrayND(ArrayND::Bool(array.clone()))),
            }),
            ArrayND::Str(array) => Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::ArrayND(ArrayND::Str(array.clone()))),
            }),
        },
        Value::Hashmap(_hashmap) => panic!("nature inference is not implemented for hashmaps"),
        Value::Vector2DJagged(jagged) => match jagged {
            Vector2DJagged::F64(jagged) => Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))),
            }),
            Vector2DJagged::I64(jagged) => Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))),
            }),
            Vector2DJagged::Bool(jagged) => Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Bool(jagged.clone()))),
            }),
            Vector2DJagged::Str(jagged) => Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Str(jagged.clone()))),
            }),
        }
    }
}

pub fn infer_nullity(_value: &Value) -> Result<bool> {
    Ok(true)
}

pub fn infer_c_stability(value: &Value) -> Result<Vec<f64>> {
    Ok(match infer_num_columns(&value)? {
        Some(num_columns) => (0..num_columns).map(|_| 1.).collect(),
        None => Vec::new()
    })
}

pub fn infer_property(value: &Value) -> Result<ValueProperties> {
    Ok(match value {
        Value::ArrayND(array) => ArrayNDProperties {
            nullity: infer_nullity(&value)?,
            releasable: true,
            nature: Some(infer_nature(&value)),
            c_stability: infer_c_stability(&value)?,
            num_columns: infer_num_columns(&value)?,
            num_records: infer_num_rows(array)?,
            aggregator: None,
            data_type: match array {
                ArrayND::Bool(_) => DataType::Bool,
                ArrayND::F64(_) => DataType::F64,
                ArrayND::I64(_) => DataType::I64,
                ArrayND::Str(_) => DataType::Str,
            }
        }.into(),
        Value::Hashmap(hashmap) => HashmapProperties {
            num_records: None,
            disjoint: false,
            value_properties: match hashmap {
                Hashmap::Str(hashmap) => hashmap.iter()
                    .map(|(name, value)| infer_property(value)
                        .map(|v| (name.clone(), v)))
                    .collect::<Result<HashMap<String, ValueProperties>>>()?.into(),
                Hashmap::I64(hashmap) => hashmap.iter()
                    .map(|(name, value)| infer_property(value)
                        .map(|v| (name.clone(), v)))
                    .collect::<Result<HashMap<i64, ValueProperties>>>()?.into(),
                Hashmap::Bool(hashmap) => hashmap.iter()
                    .map(|(name, value)| infer_property(value)
                        .map(|v| (name.clone(), v)))
                    .collect::<Result<HashMap<bool, ValueProperties>>>()?.into(),
            }
        }.into(),
        Value::Vector2DJagged(jagged) => Vector2DJaggedProperties {}.into()
    })
}