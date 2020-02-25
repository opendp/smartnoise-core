use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NatureCategorical, Nature, NatureContinuous};


use crate::proto;

use crate::components::Component;
use ndarray::{Axis};
use ndarray::prelude::*;
use ndarray_stats::QuantileExt;
use crate::utilities::serial::{parse_value, Value, ArrayND, Vector2DJagged, Vector1DNull};

use itertools::Itertools;
use std::cmp::Ordering;



pub fn get_shape(array: &ArrayND) -> Vec<i64> {
    match array {
        ArrayND::Bool(array) => array.shape().to_owned(),
        ArrayND::F64(array) => array.shape().to_owned(),
        ArrayND::I64(array) => array.shape().to_owned(),
        ArrayND::Str(array) => array.shape().to_owned()
    }.iter().map(|arr| arr.clone() as i64).collect()
}

pub fn infer_num_columns(value: &Value) -> Result<Option<i64>, String> {
    match value {
        Value::ArrayND(array) => {
            let shape = get_shape(&array);
            match shape.len() {
                0 => Ok(None),
                1 => Ok(Some(shape[0])),
                2 => Ok(Some(shape[1])),
                _ => Err("arrays may have max dimensionality of 2".to_owned())
            }
        },
        Value::HashmapString(hashmap) => Ok(Some(hashmap.len() as i64)),
        Value::Vector2DJagged(vector) => Ok(Some(match vector {
            Vector2DJagged::Bool(vector) => vector.len(),
            Vector2DJagged::F64(vector) => vector.len(),
            Vector2DJagged::I64(vector) => vector.len(),
            Vector2DJagged::Str(vector) => vector.len(),
        } as i64))
    }
}
pub fn infer_num_rows(value: &Value) -> Result<Vec<Option<i64>>, String> {
    match value {
        Value::ArrayND(array) => {
            let shape = get_shape(array);
            match shape.len() {
                0 => Ok(vec![None]),
                1 => Ok((0..shape[0]).collect::<Vec<i64>>().iter().map(|_| Some(1)).collect()),
                2 => Ok((0..shape[1]).collect::<Vec<i64>>().iter().map(|_| Some(shape[0])).collect()),
                _ => Err("arrays may have max dimensionality of 2".to_owned())
            }
        },
        Value::HashmapString(hashmap) => hashmap.values().map(|value| match value {
            Value::ArrayND(array) => {
                let shape = get_shape(&array);
                match shape.len() {
                    0 => Ok(Some(1)),
                    1 => Ok(Some(1)),
                    2 => Ok(Some(shape[0])),
                    _ => Err("arrays may have max dimensionality of 2".to_owned())
                }
            },
            _ => Err("Constraints on hashmaps are only implemented for single-column arrays".to_string())
        }).collect(),
        Value::Vector2DJagged(jagged) => Ok(match jagged {
            Vector2DJagged::Bool(vector) => vector.iter()
                .map(|col| match col {
                    Some(vec) => Some(vec.len() as i64),
                    None => None
                }).collect(),
            Vector2DJagged::F64(vector) => vector.iter()
                .map(|col| match col {
                    Some(vec) => Some(vec.len() as i64),
                    None => None
                }).collect(),
            Vector2DJagged::I64(vector) => vector.iter()
                .map(|col| match col {
                    Some(vec) => Some(vec.len() as i64),
                    None => None
                }).collect(),
            Vector2DJagged::Str(vector) => vector.iter()
                .map(|col| match col {
                    Some(vec) => Some(vec.len() as i64),
                    None => None
                }).collect(),
        })
    }
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
        Value::HashmapString(hashmap) => {
            let mut bound: Vec<Option<f64>> = vec![];
            hashmap.values()
                .map(infer_min)
                .for_each(|next| bound.extend(next));
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
        Value::HashmapString(hashmap) => {
            let mut bound: Vec<Option<f64>> = vec![];
            hashmap.values()
                .map(infer_max)
                .for_each(|next| bound.extend(next));
            bound
        }
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
        Value::HashmapString(_hashmap) => panic!("category inference is not implemented for hashmaps"),
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
        Value::HashmapString(_hashmap) => panic!("nature inference is not implemented for hashmaps"),
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

pub fn infer_nullity(_value: &Value) -> Result<bool, String> {
    Ok(true)
}

pub fn infer_c_stability(_value: &Value) -> Result<Vec<f64>, String> {
    Ok(vec![])
}

pub fn infer_constraint(value: &Value) -> Result<Constraint, String> {
    Ok(Constraint {
        nullity: infer_nullity(&value)?,
        releasable: true,
        nature: Some(infer_nature(&value)),
        c_stability: infer_c_stability(&value)?,
        num_columns: infer_num_columns(&value)?,
        num_records: infer_num_rows(&value)?
    })
}

impl Component for proto::Literal {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let value = parse_value(&self.value.clone().unwrap()).unwrap();

        match self.private {
            true => {
                let num_columns = infer_num_columns(&value)?;

                Ok(Constraint {
                    num_records: match num_columns {
                        Some(num_cols) => (0..num_cols).collect::<Vec<i64>>().iter().map(|_v| None).collect(),
                        None => vec![Some(1)]
                    },
                    num_columns: infer_num_columns(&value)?,
                    nullity: true,
                    releasable: false,
                    c_stability: match num_columns {
                        Some(num_cols) => (0..num_cols).collect::<Vec<i64>>().iter().map(|_| 1.).collect(),
                        None => vec![1.]
                    },
                    nature: None,
                })
            },
            false => infer_constraint(&value)
        }
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        true
    }
}
