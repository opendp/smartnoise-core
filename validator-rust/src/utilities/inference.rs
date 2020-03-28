//! Infer ValueProperties from a public Value
//!
//! When public arguments are provided, the properties about those public arguments are not known.
//! These utility functions provide a conversion from Value to ValueProperties.

use crate::errors::*;


use ndarray::{Axis};
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use crate::base::{Array, Value, Jagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayProperties, DataType, HashmapProperties, JaggedProperties, Hashmap};

use std::collections::{HashMap};
use crate::utilities::deduplicate;

pub fn infer_min(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {
            match array.shape().len() as i64 {
                0 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(vec![Some(array.first().ok_or::<Error>("min may not have length zero".into())?.to_owned())]),
                    Array::I64(array) =>
                        Vector1DNull::I64(vec![Some(array.first().ok_or::<Error>("min may not have length zero".into())?.to_owned())]),
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                },
                1 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.iter().map(|v| Some(*v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.iter().map(|v| Some(*v)).collect()),
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                },
                2 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| v.clone()).map_err(|e| e.into()))
                            .collect::<Result<Vec<f64>>>()?
                            .into_iter().map(|v| Some(v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| v.clone()).map_err(|e| e.into()))
                            .collect::<Result<Vec<i64>>>()?
                            .into_iter().map(|v| Some(v)).collect()),
                    _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        },
        Value::Hashmap(_hashmap) => return Err("constraint inference is not implemented for hashmaps".into()),
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::F64(jagged) => Vector1DNull::F64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(col.iter().map(|v| v.clone()).fold1(|l, r| l.min(r))
                        .ok_or::<Error>("attempted to infer min on an empty value".into())?.clone()),
                    None => None
                })).collect::<Result<_>>()?),
                Jagged::I64(jagged) => Vector1DNull::I64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::min)
                        .ok_or::<Error>("attempted to infer min on an empty value".into())?),
                    None => None
                })).collect::<Result<_>>()?),
                _ => return Err("Cannot infer numeric min on a non-numeric vector".into())
            }
        }
    })
}
pub fn infer_max(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {

            match array.shape().len() as i64 {
                0 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(vec![Some(array.first()
                            .ok_or::<Error>("min may not have length zero".into())?.to_owned())]),
                    Array::I64(array) =>
                        Vector1DNull::I64(vec![Some(array.first()
                            .ok_or::<Error>("min may not have length zero".into())?.to_owned())]),
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                },
                1 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.iter().map(|v| Some(*v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.iter().map(|v| Some(*v)).collect()),
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                },
                2 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| v.clone()).map_err(|e| e.into()))
                            .collect::<Result<Vec<f64>>>()?
                            .into_iter().map(|v| Some(v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| v.clone()).map_err(|e| e.into()))
                            .collect::<Result<Vec<i64>>>()?
                            .into_iter().map(|v| Some(v)).collect()),
                    _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        },
        Value::Hashmap(_hashmap) => return Err("constraint inference is not implemented for hashmaps".into()),
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::F64(jagged) => Vector1DNull::F64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(col.iter().map(|x| x.clone()).fold1(|l, r| l.max(r).clone())
                        .ok_or::<Error>("attempted to infer max on an empty value".into())?),
                    None => None
                })).collect::<Result<_>>()?),
                Jagged::I64(jagged) => Vector1DNull::I64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::max)
                        .ok_or::<Error>("attempted to infer max on an empty value".into())?),
                    None => None
                })).collect::<Result<_>>()?),
                _ => return Err("Cannot infer numeric max on a non-numeric vector".into())
            }
        }
    })
}

pub fn infer_categories(value: &Value) -> Result<Jagged> {
    Ok(match value {
        Value::Array(array) => match array {
            Array::Bool(array) =>
                Jagged::Bool(array.gencolumns().into_iter().map(|col|
                    Ok(Some(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec())))
                    .collect::<Result<Vec<_>>>()?),
            Array::F64(array) =>
                Jagged::F64(array.gencolumns().into_iter().map(|col|
                    Ok(Some(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec())))
                    .collect::<Result<Vec<_>>>()?),
            Array::I64(array) =>
                Jagged::I64(array.gencolumns().into_iter().map(|col|
                    Ok(Some(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec())))
                    .collect::<Result<Vec<_>>>()?),
            Array::Str(array) =>
                Jagged::Str(array.gencolumns().into_iter().map(|col|
                    Ok(Some(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec())))
                    .collect::<Result<Vec<_>>>()?),
        },
        Value::Hashmap(_) => return Err("category inference is not implemented for hashmaps".into()),
        Value::Jagged(jagged) => match jagged {
            Jagged::Bool(array) =>
                Jagged::Bool(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => Some(deduplicate(column_categories.to_owned())),
                    None => None
                }).collect()),
            // TODO: consider removing support for float categories
            Jagged::F64(array) =>
                Jagged::F64(array.iter().map(|_| None).collect()),
            Jagged::I64(array) =>
                Jagged::I64(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => Some(deduplicate(column_categories.to_owned())),
                    None => None
                }).collect()),
            Jagged::Str(array) =>
                Jagged::Str(array.iter().map(|column_categories| match column_categories {
                    Some(column_categories) => Some(deduplicate(column_categories.to_owned())),
                    None => None
                }).collect()),
        }
    })
}

pub fn infer_nature(value: &Value) -> Result<Option<Nature>> {
    Ok(match value {
        Value::Array(array) => match array {
            Array::F64(array) => Some(Nature::Continuous(NatureContinuous {
                min: infer_min(&array.clone().into())?,
                max: infer_max(&array.clone().into())?,
            })),
            Array::I64(array) => Some(Nature::Continuous(NatureContinuous {
                min: infer_min(&array.clone().into())?,
                max: infer_max(&array.clone().into())?,
            })),
            Array::Bool(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
            })),
            // This has a nasty side-effect of duplicating columns within the properties
//            ArrayND::Str(array) => Nature::Categorical(NatureCategorical {
//                categories: infer_categories(&Value::ArrayND(ArrayND::Str(array.clone()))),
//            }),
            _ => None
        },
        Value::Hashmap(_) => None,
        Value::Jagged(jagged) => match jagged {
            Jagged::F64(_) => None,
            _ => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(value)?,
            }))
        }
    })
}

pub fn infer_nullity(value: &Value) -> Result<bool> {
    match value {
        Value::Array(value) => match value {
            Array::F64(value) => Ok(value.iter().any(|v| v.is_nan())),
            _ => Ok(false)
        },
        _ => Ok(false)
    }
}

pub fn infer_c_stability(value: &Array) -> Result<Vec<f64>> {
    Ok((0..value.num_columns()?).map(|_| 1.).collect())
}

pub fn infer_property(value: &Value) -> Result<ValueProperties> {
    Ok(match value {
        Value::Array(array) => ArrayProperties {
            nullity: infer_nullity(&value)?,
            releasable: true,
            nature: infer_nature(&value)?,
            c_stability: infer_c_stability(&array)?,
            num_columns: Some(array.num_columns()?),
            num_records: Some(array.num_records()?),
            aggregator: None,
            data_type: match array {
                Array::Bool(_) => DataType::Bool,
                Array::F64(_) => DataType::F64,
                Array::I64(_) => DataType::I64,
                Array::Str(_) => DataType::Str,
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
        Value::Jagged(_jagged) => JaggedProperties {
            releasable: true
        }.into()
    })
}