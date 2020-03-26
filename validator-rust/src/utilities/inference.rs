//! Infer ValueProperties from a public Value
//!
//! When public arguments are provided, the properties about those public arguments are not known.
//! These utility functions provide a conversion from Value to ValueProperties.

use crate::errors::*;


use ndarray::{Axis};
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use std::cmp::Ordering;
use crate::base::{ArrayND, Value, Vector2DJagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayNDProperties, DataType, HashmapProperties, Vector2DJaggedProperties, Hashmap};

use std::collections::HashMap;

pub fn infer_min(value: &Value) -> Result<Vec<Option<f64>>> {
    Ok(match value {
        Value::ArrayND(array) => {
            match array.get_shape().len() as i64 {
                0 => vec![Some(match array {
                    ArrayND::F64(array) =>
                        array.first().unwrap().to_owned(),
                    ArrayND::I64(array) =>
                        array.first().unwrap().to_owned() as f64,
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                })],
                1 => match array {
                    ArrayND::F64(array) =>
                        array.iter().map(|v| Some(*v)).collect(),
                    ArrayND::I64(array) =>
                        array.iter().map(|v| Some(*v as f64)).collect(),
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                },
                2 => match array {
                    ArrayND::F64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(col.max().unwrap().clone())).collect(),
                    ArrayND::I64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(*col.max().unwrap() as f64)).collect(),
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        },
        Value::Hashmap(_hashmap) => {
            let bound: Vec<Option<f64>> = vec![];
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
                _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
            }
        }
    })
}
pub fn infer_max(value: &Value) -> Result<Vec<Option<f64>>> {
    Ok(match value {
        Value::ArrayND(array) => {

            match array.get_shape().len() as i64 {
                0 => vec![Some(match array {
                    ArrayND::F64(array) =>
                        array.first().unwrap().to_owned(),
                    ArrayND::I64(array) =>
                        array.first().unwrap().to_owned() as f64,
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                })],
                1 => match array {
                    ArrayND::F64(array) =>
                        array.iter().map(|v| Some(*v)).collect(),
                    ArrayND::I64(array) =>
                        array.iter().map(|v| Some(*v as f64)).collect(),
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                },
                2 => match array {
                    ArrayND::F64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(col.max().unwrap().clone())).collect(),
                    ArrayND::I64(array) =>
                        array.lanes(Axis(0)).into_iter().map(|col| Some(*col.max().unwrap() as f64)).collect(),
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        },
        Value::Hashmap(_hashmap) => return Err("max inference is not compatible with a hashmap".into()),
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
                _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
            }
        }
    })
}


pub fn infer_categories(value: &Value) -> Result<Vector2DJagged> {
    Ok(match value {
        Value::ArrayND(array) => match array {
            ArrayND::Bool(array) =>
                Vector2DJagged::Bool(array.gencolumns().into_iter().map(|col| {
                    let column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::F64(array) =>
                Vector2DJagged::F64(array.gencolumns().into_iter().map(|col| {
                    let column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::I64(array) =>
                Vector2DJagged::I64(array.gencolumns().into_iter().map(|col| {
                    let column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect()),
            ArrayND::Str(array) =>{

//                println!("array in inference {:?}", array);
                Vector2DJagged::Str(array.gencolumns().into_iter().map(|col| {
                    let column_categories = col.into_dyn().
                        into_dimensionality::<Ix1>().unwrap().to_vec();
//                    column_categories.sort();
//                    column_categories.dedup();
                    Some(column_categories)
                }).collect())
            }
        },
        Value::Hashmap(_hashmap) => return Err("category inference is not implemented for hashmaps".into()),
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
    })
}

pub fn infer_nature(value: &Value) -> Result<Option<Nature>> {
    Ok(match value {
        Value::ArrayND(array) => match array {
            ArrayND::F64(array) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&array.clone().into())?),
                max: Vector1DNull::F64(infer_max(&array.clone().into())?),
            })),
            ArrayND::I64(array) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&array.clone().into())?),
                max: Vector1DNull::F64(infer_max(&array.clone().into())?),
            })),
            ArrayND::Bool(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
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
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))?),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::F64(jagged.clone())))?),
            })),
            Vector2DJagged::I64(jagged) => Some(Nature::Continuous(NatureContinuous {
                min: Vector1DNull::F64(infer_min(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))?),
                max: Vector1DNull::F64(infer_max(&Value::Vector2DJagged(Vector2DJagged::I64(jagged.clone())))?),
            })),
            Vector2DJagged::Bool(jagged) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Bool(jagged.clone())))?,
            })),
            Vector2DJagged::Str(jagged) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&Value::Vector2DJagged(Vector2DJagged::Str(jagged.clone())))?,
            })),
        }
    })
}

pub fn infer_nullity(value: &Value) -> Result<bool> {
    match value {
        Value::ArrayND(value) => match value {
            ArrayND::F64(value) => Ok(value.iter().any(|v| v.is_nan())),
            _ => Ok(false)
        },
        _ => Ok(false)
    }
}

pub fn infer_c_stability(value: &ArrayND) -> Result<Vec<f64>> {
    Ok((0..value.get_num_columns()?).map(|_| 1.).collect())
}

pub fn infer_property(value: &Value) -> Result<ValueProperties> {
    Ok(match value {
        Value::ArrayND(array) => ArrayNDProperties {
            nullity: infer_nullity(&value)?,
            releasable: true,
            nature: infer_nature(&value)?,
            c_stability: infer_c_stability(&array)?,
            num_columns: Some(array.get_num_columns()?),
            num_records: Some(array.get_num_records()?),
            aggregator: None,
            data_type: match array {
                ArrayND::Bool(_) => DataType::Bool,
                ArrayND::F64(_) => DataType::F64,
                ArrayND::I64(_) => DataType::I64,
                ArrayND::Str(_) => DataType::Str,
            },
            dataset_id: None
        }.into(),
        Value::Hashmap(hashmap) => HashmapProperties {
            num_records: None,
            disjoint: false,
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
            },
            columnar: false
        }.into(),
        Value::Vector2DJagged(_jagged) => Vector2DJaggedProperties {
            releasable: true
        }.into()
    })
}