//! Infer ValueProperties from a public Value
//!
//! When public arguments are provided, the properties about those public arguments are not known.
//! These utility functions provide a conversion from Value to ValueProperties.

use crate::errors::*;

use crate::{Float, Integer};
use ndarray::Axis;
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;

use itertools::Itertools;
use crate::base::{Array, Value, Jagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, ValueProperties, ArrayProperties, DataType, JaggedProperties, IndexKey, DataframeProperties, PartitionsProperties};

use crate::utilities::deduplicate;
use indexmap::map::IndexMap;

pub fn infer_lower(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {
            match array.shape().len() as i64 {
                0 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(vec![Some(array.first()
                            .ok_or_else(|| Error::from("lower bounds may not be length zero"))?.to_owned())]),
                    Array::Int(array) =>
                        Vector1DNull::Int(vec![Some(array.first()
                            .ok_or_else(|| Error::from("lower bounds may not be length zero"))?.to_owned())]),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                1 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(vec![array.iter().cloned()
                            .fold1(|lowest, v| lowest.min(v))]),
                    Array::Int(array) =>
                        Vector1DNull::Int(vec![array.iter().max().cloned()]),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                2 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<Float>>>()?
                            .into_iter().map(Some).collect()),
                    Array::Int(array) =>
                        Vector1DNull::Int(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.min().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<Integer>>>()?
                            .into_iter().map(Some).collect()),
                    _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        }
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::Float(jagged) => Vector1DNull::Float(jagged.iter()
                    .map(|col| col.iter().copied().fold1(|l, r| l.min(r))
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value")))
                    .collect::<Result<Vec<Float>>>()?.into_iter().map(Some).collect()),
                Jagged::Int(jagged) => Vector1DNull::Int(jagged.iter()
                    .map(|col| col.iter().min()
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value")))
                    .collect::<Result<Vec<&Integer>>>()?.into_iter().copied().map(Some).collect()),
                _ => return Err("Cannot infer numeric lower bounds on a non-numeric vector".into())
            }
        }
        _ => return Err("bounds inference is only implemented for arrays and jagged arrays".into())
    })
}

pub fn infer_upper(value: &Value) -> Result<Vector1DNull> {
    Ok(match value {
        Value::Array(array) => {
            match array.shape().len() as i64 {
                0 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(vec![Some(array.first()
                            .ok_or_else(|| Error::from("upper bounds may not be length zero"))?.to_owned())]),
                    Array::Int(array) =>
                        Vector1DNull::Int(vec![Some(array.first()
                            .ok_or_else(|| Error::from("upper bounds may not be length zero"))?.to_owned())]),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                1 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(vec![array.iter().cloned()
                            .fold1(|greatest, v| greatest.max(v))]),
                    Array::Int(array) =>
                        Vector1DNull::Int(vec![array.iter().max().cloned()]),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                2 => match array {
                    Array::Float(array) =>
                        Vector1DNull::Float(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<Float>>>()?
                            .into_iter().map(Some).collect()),
                    Array::Int(array) =>
                        Vector1DNull::Int(array.lanes(Axis(0)).into_iter()
                            .map(|col| col.max().map(|v| *v).map_err(|e| e.into()))
                            .collect::<Result<Vec<Integer>>>()?
                            .into_iter().map(Some).collect()),
                    _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
                },
                _ => return Err("arrays may have max dimensionality of 2".into())
            }
        }
        Value::Jagged(jagged) => {
            match jagged {
                Jagged::Float(jagged) => Vector1DNull::Float(jagged.iter()
                    .map(|col| col.iter().copied().fold1(|l, r| l.max(r))
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value")))
                    .collect::<Result<Vec<Float>>>()?.into_iter().map(Some).collect()),
                Jagged::Int(jagged) => Vector1DNull::Int(jagged.iter()
                    .map(|col| col.iter().max()
                        .ok_or_else(|| Error::from("attempted to infer lower bounds on an empty value")))
                    .collect::<Result<Vec<&Integer>>>()?.into_iter().copied().map(Some).collect()),
                _ => return Err("Cannot infer numeric upper bounds on a non-numeric vector".into())
            }
        }
        _ => return Err("bounds inference is only implemented for arrays and jagged arrays".into())
    })
}

pub fn infer_categories(value: &Value) -> Result<Jagged> {
    match value {
        Value::Array(array) => match array {
            Array::Bool(array) =>
                Jagged::Bool(array.gencolumns().into_iter().map(|col|
                    Ok(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec()))
                    .collect::<Result<Vec<_>>>()?),
            Array::Float(array) =>
                Jagged::Float(array.gencolumns().into_iter().map(|col|
                    Ok(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec()))
                    .collect::<Result<Vec<_>>>()?),
            Array::Int(array) =>
                Jagged::Int(array.gencolumns().into_iter().map(|col|
                    Ok(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec()))
                    .collect::<Result<Vec<_>>>()?),
            Array::Str(array) =>
                Jagged::Str(array.gencolumns().into_iter().map(|col|
                    Ok(col.into_dyn().into_dimensionality::<Ix1>()?.to_vec()))
                    .collect::<Result<Vec<_>>>()?),
        }
        Value::Jagged(jagged) => match jagged {
            Jagged::Bool(array) =>
                Jagged::Bool(array.iter().cloned().map(deduplicate).collect()),
            Jagged::Float(_array) =>
                return Err("categories are not defined for floats".into()),
            Jagged::Int(array) =>
                Jagged::Int(array.iter().cloned().map(deduplicate).collect()),
            Jagged::Str(array) =>
                Jagged::Str(array.iter().cloned().map(deduplicate).collect()),
        }
        _ => return Err("category inference is only implemented for arrays and jagged arrays".into()),
    }.deduplicate()
}

pub fn infer_nature(
    value: &Value, prior_property: Option<&ValueProperties>
) -> Result<Option<Nature>> {
    Ok(match value {
        Value::Array(array) => match array {
            Array::Float(array) => Some(Nature::Continuous(NatureContinuous {
                lower: infer_lower(&array.clone().into())?,
                upper: infer_upper(&array.clone().into())?,
            })),
            Array::Int(array) => {
                let is_categorical = match prior_property {
                    Some(p) => p.array()?.clone().nature.map(|nature| match nature {
                        Nature::Categorical(_) => true,
                        Nature::Continuous(_) => false
                    }).unwrap_or(false),
                    None => false
                };
                if is_categorical {
                    Some(Nature::Categorical(NatureCategorical {
                        categories: infer_categories(&array.clone().into())?
                    }))
                } else {
                    Some(Nature::Continuous(NatureContinuous {
                        lower: infer_lower(&array.clone().into())?,
                        upper: infer_upper(&array.clone().into())?,
                    }))
                }

            },
            Array::Bool(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
            })),
            Array::Str(array) => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(&array.clone().into())?,
            })),
        },
        Value::Jagged(jagged) => match jagged {
            Jagged::Float(_) => None,
            _ => Some(Nature::Categorical(NatureCategorical {
                categories: infer_categories(value)?,
            }))
        },
        _ => return Err("nature inference is only implemented for arrays and jagged arrays".into())
    })
}

pub fn infer_nullity(value: &Value) -> Result<bool> {
    match value {
        Value::Array(value) => match value {
            Array::Float(value) => Ok(value.iter().any(|v| !v.is_finite())),
            _ => Ok(false)
        },
        _ => Ok(false)
    }
}

pub fn infer_property(
    value: &Value, prior_property: Option<&ValueProperties>, node_id: u32
) -> Result<ValueProperties> {

    Ok(match value {
        Value::Array(array) => {
            let prior_prop_arr = match prior_property {
                Some(p) => Some(p.array()?),
                None => None
            };
            ArrayProperties {
                nullity: infer_nullity(&value)?,
                releasable: true,
                nature: infer_nature(&value, prior_property)?,
                c_stability: prior_prop_arr
                    .map(|prop| prop.c_stability)
                    .unwrap_or(1),
                num_columns: Some(array.num_columns()? as i64),
                num_records: Some(array.num_records()? as i64),
                aggregator: prior_prop_arr.and_then(|p| p.aggregator.clone()),
                data_type: match array {
                    Array::Bool(_) => DataType::Bool,
                    Array::Float(_) => DataType::Float,
                    Array::Int(_) => DataType::Int,
                    Array::Str(_) => DataType::Str,
                },
                dataset_id: prior_prop_arr.and_then(|p| p.dataset_id),
                node_id: node_id as i64,
                is_not_empty: array.num_records()? != 0,
                dimensionality: Some(array.shape().len() as i64),
                group_id: prior_prop_arr
                    .map(|v| v.group_id.clone())
                    .unwrap_or_else(Vec::new),
                naturally_ordered: true,
                sample_proportion: prior_prop_arr.and_then(|p| p.sample_proportion)
            }.into()
        },
        Value::Dataframe(dataframe) => match prior_property {
            Some(ValueProperties::Dataframe(prior_property)) =>
                DataframeProperties {
                    children: dataframe.iter()
                        .zip(prior_property.children.values())
                        .map(|((name, value), prop)|
                            infer_property(value, Some(prop), node_id)
                                .map(|v| (name.clone(), v)))
                        .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
                }.into(),
            Some(_) => return Err("the prior properties for the dataframe do not match the actual data".into()),
            None =>
                DataframeProperties {
                    children: dataframe.iter()
                        .map(|(name, value)| infer_property(value, None, node_id)
                            .map(|v| (name.clone(), v)))
                        .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
                }.into()
        }
        Value::Partitions(partitions) => match prior_property {
            Some(ValueProperties::Partitions(prior_property)) =>
                PartitionsProperties {
                    children: partitions.iter()
                        .zip(prior_property.children.values())
                        .map(|((name, value), prop)|
                            infer_property(value, Some(prop), node_id)
                                .map(|v| (name.clone(), v)))
                        .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
                }.into(),
            Some(_) => return Err("the prior properties for the partitions do not match the actual data".into()),
            None =>
                PartitionsProperties {
                    children: partitions.iter()
                        .map(|(name, value)| infer_property(value, None, node_id)
                            .map(|v| (name.clone(), v)))
                        .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
                }.into()
        }
        Value::Jagged(jagged) => JaggedProperties {
            num_records: Some(jagged.num_records()),
            nullity: match &jagged {
                Jagged::Float(jagged) => jagged.iter()
                    .any(|col| col.iter()
                        .any(|elem| !elem.is_finite())),
                _ => false
            },
            aggregator: None,
            nature: infer_nature(value, prior_property)?,
            data_type: match jagged {
                Jagged::Bool(_) => DataType::Bool,
                Jagged::Float(_) => DataType::Float,
                Jagged::Int(_) => DataType::Int,
                Jagged::Str(_) => DataType::Str,
            },
            releasable: true
        }.into(),
        // TODO: custom properties for Functions (may not be needed)
        Value::Function(_function) => unreachable!()
    })
}