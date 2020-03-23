use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument, Hashmap, DataType};
use crate::components::Evaluable;
use whitenoise_validator::proto;
use crate::utilities::array::{slow_stack, slow_select};
use ndarray::prelude::*;
use std::collections::HashMap;

use whitenoise_validator::components::index::{to_name_vec, mask_columns};


impl Evaluable for proto::Index {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?;
        let columns = get_argument(&arguments, "columns")?.get_arraynd()?;

        match data {
            // if value is a hashmap, we'll be stacking arrays column-wise
            Value::Hashmap(dataframe) => match dataframe {
                Hashmap::Str(dataframe) => match columns {
                    ArrayND::Str(columns) => column_stack(
                        dataframe, &to_name_vec(columns)?),
                    ArrayND::I64(columns) => {
                        let column_names = dataframe.keys().cloned().collect::<Vec<String>>();
                        let columns = to_name_vec(columns)?.iter()
                            .map(|index| column_names.get(*index as usize).cloned()
                                .ok_or::<Error>("column index out of bounds".into())).collect::<Result<Vec<String>>>()?;
                        column_stack(dataframe, &columns)
                    },
                    ArrayND::Bool(columns) => column_stack(dataframe, &mask_columns(
                        &dataframe.keys().cloned().collect::<Vec<String>>(),
                        &to_name_vec(columns)?)?),
                    _ => Err("the data type of the column headers is not supported".into())
                },
                Hashmap::I64(dataframe) => {
                    match columns {
                        ArrayND::I64(columns) => column_stack(dataframe, &to_name_vec(columns)?),
                        ArrayND::Bool(columns) => column_stack(dataframe, &mask_columns(
                            &dataframe.keys().cloned().collect::<Vec<i64>>(),
                            &to_name_vec(columns)?)?),
                        _ => Err("the data type of the column headers is not supported".into())
                    }
                },
                Hashmap::Bool(dataframe) => {
                    let columns = columns.get_bool()?;
                    column_stack(dataframe, &to_name_vec(columns)?)
                }
            },

            // if the value is an array, we'll be selecting columns
            Value::ArrayND(array) => {
                let indices = match columns {
                    ArrayND::Bool(mask) => to_name_vec(mask)?.into_iter().enumerate()
                        .filter(|(_, mask)| *mask)
                        .map(|(idx, _)| idx)
                        .collect::<Vec<usize>>(),
                    ArrayND::I64(indices) => to_name_vec(indices)?.into_iter()
                        .map(|v| v as usize).collect(),
                    _ => return Err("the data type of the indices are not supported".into())
                };
                Ok(match array {
                    ArrayND::I64(data) => data.select(Axis(1), &indices).into(),
                    ArrayND::F64(data) => data.select(Axis(1), &indices).into(),
                    ArrayND::Bool(data) => data.select(Axis(1), &indices).into(),
                    ArrayND::Str(data) => slow_select(data, Axis(1), &indices).into(),
                })
            }
            Value::Vector2DJagged(_) => Err("indexing is not supported for jagged arrays".into())
        }
    }
}

fn column_stack<T: Clone + Eq + std::hash::Hash>(
    dataframe: &HashMap<T, Value>, column_names: &Vec<T>
) -> Result<Value> {
    if column_names.len() == 1 {
        return dataframe.get(column_names.first().unwrap()).cloned()
            .ok_or::<Error>("the provided column name does not exist".into())
    }

    let values = column_names.iter()
        .map(|column_name| dataframe.get(column_name))
        .collect::<Option<Vec<&Value>>>()
        .ok_or::<Error>("one of the provided column names does not exist".into())?;

    let data_type = match values.first() {
        Some(value) => match value.get_arraynd()? {
            ArrayND::F64(_) => DataType::F64,
            ArrayND::I64(_) => DataType::I64,
            ArrayND::Bool(_) => DataType::Bool,
            ArrayND::Str(_) => DataType::Str,
        },
        None => return Err("at least one column must be supplied to Index".into())
    };

    match data_type {
        DataType::F64 => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.get_arraynd()?.get_f64()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::I64 => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.get_arraynd()?.get_i64()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Bool => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.get_arraynd()?.get_bool()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Str => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.get_arraynd()?.get_str()?.clone())))
                .collect::<Result<Vec<ArrayD<String>>>>()?;

            Ok(slow_stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<String>>>())?.into())
        }
    }
}

fn to_nd<T>(mut array: ArrayD<T>, ndim: &usize) -> Result<ArrayD<T>> {
    match (*ndim as i32) - (array.ndim() as i32) {
        0 => {},
        // must remove i axes
        i if i < 0 => {
            (0..-(i as i32)).map(|_| match array.shape().last().ok_or::<Error>("ndim may not be negative".into())? {
                1 => Ok(array.index_axis_inplace(Axis(array.ndim().clone()), 0)),
                _ => Err("cannot remove non-singleton trailing axis".into())
            }).collect::<Result<_>>()?
        },
        // must add i axes
        i if i > 0 => (0..i).for_each(|idx| array.insert_axis_inplace(Axis((idx + 1) as usize))),
        _ => return Err("invalid dimensionality".into())
    };

    Ok(array)
}

fn to_2d<T>(array: ArrayD<T>) -> Result<ArrayD<T>> {
    to_nd(array, &(2 as usize))
}
