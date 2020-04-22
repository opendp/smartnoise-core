//! Infer ValueProperties from a public Value
//!
//! When public arguments are provided, the properties about those public arguments are not known.
//! These utility functions provide a conversion from Value to ValueProperties.

use crate::errors::*;


use ndarray::Axis;
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use crate::base::{Array, Value, Jagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayProperties, DataType, HashmapProperties, JaggedProperties, Hashmap};

use std::collections::BTreeMap;
use crate::utilities::deduplicate;

pub fn infer_lower(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {
            match array.shape().len() as i64 {
                0 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(vec![
                            Some(array.first().ok_or_else(|| Error::from("lower bounds may not be length zero"))?.to_owned())]),
                    Array::I64(array) =>
                        Vector1DNull::I64(vec![
                            Some(array.first().ok_or_else(|| Error::from("lower bounds may not be length zero"))?.to_owned())]),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                1 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.iter().map(|v| Some(*v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.iter().map(|v| Some(*v)).collect()),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                2 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<f64>>>()?
                            .into_iter().map(Some).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<i64>>>()?
                            .into_iter().map(Some).collect()),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        }
        Value::Hashmap(_hashmap) => return Err("constraint inference is not implemented for hashmaps".into()),
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::F64(jagged) => Vector1DNull::F64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(col.iter().copied().fold1(|l, r| l.min(r))
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value"))?),
                    None => None
                })).collect::<Result<_>>()?),
                Jagged::I64(jagged) => Vector1DNull::I64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::min)
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value"))?),
                    None => None
                })).collect::<Result<_>>()?),
                _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
            }
        }
    })
}

pub fn infer_upper(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {
            match array.shape().len() as i64 {
                0 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(vec![Some(array.first()
                            .ok_or_else(|| Error::from("upper bounds may not be length zero"))?.to_owned())]),
                    Array::I64(array) =>
                        Vector1DNull::I64(vec![Some(array.first()
                            .ok_or_else(|| Error::from("upper bounds may not be length zero"))?.to_owned())]),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                1 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.iter().map(|v| Some(*v)).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.iter().map(|v| Some(*v)).collect()),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                2 => match array {
                    Array::F64(array) =>
                        Vector1DNull::F64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<f64>>>()?
                            .into_iter().map(Some).collect()),
                    Array::I64(array) =>
                        Vector1DNull::I64(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<i64>>>()?
                            .into_iter().map(Some).collect()),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        }
        Value::Hashmap(_hashmap) => return Err("constraint inference is not implemented for hashmaps".into()),
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::F64(jagged) => Vector1DNull::F64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(col.iter().cloned()
                        .fold1(|l, r| l.max(r))
                        .ok_or_else(|| Error::from("attempted to infer upper bounds on an empty value"))?),
                    None => None
                })).collect::<Result<_>>()?),
                Jagged::I64(jagged) => Vector1DNull::I64(jagged.iter().map(|col| Ok(match col {
                    Some(col) => Some(*col.iter().fold1(std::cmp::max)
                        .ok_or_else(|| Error::from("attempted to infer upper bounds on an empty value"))?),
                    None => None
                })).collect::<Result<_>>()?),
                _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
            }
        }
    })
}

pub fn infer_categories(value: &Value) -> Result<Jagged> {
    match value {
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
    }.deduplicate()
}

pub fn infer_nature(value: &Value) -> Result<Option<Nature>> {
    Ok(match value {
        Value::Array(array) => match array {
            Array::F64(array) => Some(Nature::Continuous(NatureContinuous {
                lower: infer_lower(&array.clone().into())?,
                upper: infer_upper(&array.clone().into())?,
            })),
            Array::I64(array) => Some(Nature::Continuous(NatureContinuous {
                lower: infer_lower(&array.clone().into())?,
                upper: infer_upper(&array.clone().into())?,
            })),
            Array::Bool(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
            })),
            Array::Str(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
            })),
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
            Array::F64(value) => Ok(value.iter().any(|v| !v.is_finite())),
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
            dataset_id: None,
            is_not_empty: match array {
                Array::Bool(array) => array.len(),
                Array::F64(array) => array.len(),
                Array::I64(array) => array.len(),
                Array::Str(array) => array.len(),
            } != 0,
            dimensionality: array.shape().len() as u32,
        }.into(),
        Value::Hashmap(hashmap) => {
            HashmapProperties {
                num_records: None,
                disjoint: false,
                properties: match hashmap {
                    Hashmap::Str(hashmap) => hashmap.iter()
                        .map(|(name, value)| infer_property(value)
                            .map(|v| (name.clone(), v)))
                        .collect::<Result<BTreeMap<String, ValueProperties>>>()?.into(),
                    Hashmap::I64(hashmap) => hashmap.iter()
                        .map(|(name, value)| infer_property(value)
                            .map(|v| (*name, v)))
                        .collect::<Result<BTreeMap<i64, ValueProperties>>>()?.into(),
                    Hashmap::Bool(hashmap) => hashmap.iter()
                        .map(|(name, value)| infer_property(value)
                            .map(|v| (*name, v)))
                        .collect::<Result<BTreeMap<bool, ValueProperties>>>()?.into(),
                },
                columnar: false,
            }.into()
        }
        Value::Jagged(_jagged) => JaggedProperties {
            releasable: true
        }.into()
    })
}