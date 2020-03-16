use crate::errors::*;


use ndarray::{Axis};
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use std::cmp::Ordering;
use crate::base::{ArrayND, Value, Vector2DJagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayNDProperties, DataType, HashmapProperties, Vector2DJaggedProperties, Hashmap};
use crate::utilities::serial::parse_data_type;
use std::collections::HashMap;

pub fn infer_min(value: &Value) -> Vec<Option<f64>> {
    match value {
        Value::ArrayND(array) => {

            match array.get_shape().len() as i64 {
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

            match array.get_shape().len() as i64 {
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
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::F64(array) =>
                Vector2DJagged::F64(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::I64(array) =>
                Vector2DJagged::I64(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::Str(array) =>{

//                println!("array in inference {:?}", array);
                Vector2DJagged::Str(array.gencolumns().into_iter().map(|col| {
                    let mut column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect())
            }
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

pub fn infer_nature(value: &Value) -> Option<Nature> {
    match value {
        Value::ArrayND(array) => match array {
            ArrayND::F64(array) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::ArrayND(ArrayND::F64(array.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::ArrayND(ArrayND::F64(array.clone())))),
            })),
            ArrayND::I64(array) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::ArrayND(ArrayND::I64(array.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::ArrayND(ArrayND::I64(array.clone())))),
            })),
            ArrayND::Bool(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::ArrayND(ArrayND::Bool(array.clone()))),
            })),
            // This has a nasty side-effect of duplicating columns within the properties
//            ArrayND::Str(array) => Nature::Categorical(NatureCategorical {
//                categories: infer_categories(&Value::ArrayND(ArrayND::Str(array.clone()))),
//            }),
            _ => None
        },
        Value::Hashmap(_hashmap) => None,
        Value::Vector2DJagged(jagged) => match jagged {
            Vector2DJagged::F64(jagged) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))),
            })),
            Vector2DJagged::I64(jagged) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))),
            })),
            Vector2DJagged::Bool(jagged) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Bool(jagged.clone()))),
            })),
            Vector2DJagged::Str(jagged) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Str(jagged.clone()))),
            })),
        }
    }
}

pub fn infer_nullity(_value: &Value) -> Result<bool> {
    Ok(true)
}

pub fn infer_c_stability(value: &ArrayND) -> Result<Vec<f64>> {
    Ok((0..value.get_num_columns()?).map(|_| 1.).collect())
}

pub fn infer_property(value: &Value) -> Result<ValueProperties> {
    Ok(match value {
        Value::ArrayND(array) => ArrayNDProperties {
            nullity: infer_nullity(&value)?,
            releasable: true,
            nature: infer_nature(&value),
            c_stability: infer_c_stability(&array)?,
            num_columns: Some(array.get_num_columns()?),
            num_records: Some(array.get_num_records()?),
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
            columnar: false,
            properties: match hashmap {
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