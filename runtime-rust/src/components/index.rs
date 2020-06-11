use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, DataType, ReleaseNode, IndexKey};
use crate::components::Evaluable;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::array::{slow_stack, slow_select};
use ndarray::prelude::*;

use whitenoise_validator::components::index::{to_name_vec, mask_columns};
use whitenoise_validator::utilities::get_argument;
use crate::utilities::to_nd;
use indexmap::map::IndexMap;


impl Evaluable for proto::Index {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?;

        let dimensionality;

        let mut indexed = match data {
            // if value is an indexmap, we'll be stacking arrays column-wise
            Value::Indexmap(dataframe) => {
                let column_names = if let Ok(names) = get_argument(arguments, "names") {
                    dimensionality = names.array()?.shape().len() + 1;
                    match names.array()? {
                        Array::Str(names) => to_name_vec(names)?
                             .into_iter().map(IndexKey::from).collect(),
                        Array::I64(names) => to_name_vec(names)?
                            .into_iter().map(IndexKey::from).collect(),
                        Array::Bool(names) => to_name_vec(names)?
                            .into_iter().map(IndexKey::from).collect(),
                        Array::F64(_) => return Err("cannot index by floats".into()),
                    }

                } else if let Ok(indices) = get_argument(arguments, "indices") {
                    dimensionality = indices.array()?.shape().len();
                    let column_names = dataframe.keys().cloned().collect::<Vec<IndexKey>>();
                    to_name_vec(indices.array()?.i64()?)?.iter()
                        .map(|index| column_names.get(*index as usize).cloned()
                            .ok_or_else(|| Error::from("column index out of bounds"))).collect::<Result<Vec<IndexKey>>>()?

                } else if let Ok(mask) = get_argument(arguments, "mask") {
                    dimensionality = 2;
                    mask_columns(
                        &dataframe.keys().cloned().collect::<Vec<IndexKey>>(),
                        &to_name_vec(mask.array()?.bool()?)?)?

                } else {
                    return Err("names, indices, or mask must be supplied when indexing on partitions or dataframes".into())
                };

                column_stack(dataframe, &column_names)
            },

            // if the value is an array, we'll be selecting columns
            Value::Array(array) => {
                let indices = if let Ok(indices) = get_argument(arguments, "indices") {
                    let indices = indices.array()?.i64()?;
                    dimensionality = indices.shape().len();
                    to_name_vec(indices)?.into_iter()
                        .map(|v| v as usize).collect()
                } else if let Ok(mask) = get_argument(arguments, "mask") {
                    dimensionality = 2;
                    to_name_vec(mask.array()?.bool()?)?.into_iter().enumerate()
                        .filter(|(_, mask)| *mask)
                        .map(|(idx, _)| idx)
                        .collect::<Vec<usize>>()
                } else {
                    return Err("indices or mask must be supplied when indexing on arrays".into())
                };
                Ok(match array {
                    Array::I64(data) => data.select(Axis(1), &indices).into(),
                    Array::F64(data) => data.select(Axis(1), &indices).into(),
                    Array::Bool(data) => data.select(Axis(1), &indices).into(),
                    Array::Str(data) => slow_select(data, Axis(1), &indices).into(),
                })
            }
            Value::Jagged(_) => return Err("indexing is not supported for jagged arrays".into()),
            Value::Function(_) => return Err("indexing is not supported for functions".into())
        }?;

        let is_partition = get_argument(arguments, "is_partition")
            .map(|v| v.clone())
            .unwrap_or(false.into()).first_bool()?;

        // remove trailing singleton axis if a zero-dimensional index set was passed
        match &mut indexed {
            Value::Array(array) => {
                if is_partition == false && dimensionality == 1 && array.shape().len() == 2 {
                    match array {
                        Array::F64(array) => array.index_axis_inplace(Axis(1), 0),
                        Array::I64(array) => array.index_axis_inplace(Axis(1), 0),
                        Array::Bool(array) => array.index_axis_inplace(Axis(1), 0),
                        Array::Str(array) => array.index_axis_inplace(Axis(1), 0),
                    }
                }
            }
            _ => unreachable!()
        };

        Ok(ReleaseNode::new(indexed))
    }
}

fn column_stack(
    dataframe: &IndexMap<IndexKey, Value>,
    column_names: &Vec<IndexKey>,
) -> Result<Value> {
    if column_names.len() == 1 {
        return dataframe.get(column_names.first().unwrap()).cloned()
            .ok_or_else(|| Error::from("the provided column name does not exist"));
    }

    fn to_2d<T>(array: ArrayD<T>) -> Result<ArrayD<T>> {
        to_nd(array, &(2 as usize))
    }

    let values = column_names.iter()
        .map(|column_name| dataframe.get(column_name))
        .collect::<Option<Vec<&Value>>>()
        .ok_or_else(|| Error::from("one of the provided column names does not exist"))?;

    let data_type = match values.first() {
        Some(value) => match value.array()? {
            Array::F64(_) => DataType::F64,
            Array::I64(_) => DataType::I64,
            Array::Bool(_) => DataType::Bool,
            Array::Str(_) => DataType::Str,
        },
        None => return Err("at least one column must be supplied to Index".into())
    };

    match data_type {
        DataType::Unknown => unreachable!(),
        DataType::F64 => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.array()?.f64()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::I64 => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.array()?.i64()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Bool => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.array()?.bool()?.clone())))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Str => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.get(column_name)
                    .ok_or("one of the provided column names does not exist".into())
                    .and_then(|array| to_2d(array.array()?.string()?.clone())))
                .collect::<Result<Vec<ArrayD<String>>>>()?;

            Ok(slow_stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<String>>>())?.into())
        }
    }
}
