use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, DataType, ReleaseNode, IndexKey};
use crate::components::Evaluable;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::array::{slow_stack, slow_select};
use ndarray::prelude::*;

use whitenoise_validator::components::index::{to_name_vec};
use whitenoise_validator::utilities::take_argument;
use crate::utilities::to_nd;
use indexmap::map::IndexMap;


impl Evaluable for proto::Index {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?;

        let is_partition = take_argument(&mut arguments, "is_partition")
            .unwrap_or_else(|_| false.into())
            .array()?.first_bool()?;

        let dimensionality;

        let mut indexed = match data {
            // if value is an indexmap, we'll be stacking arrays column-wise
            Value::Indexmap(dataframe) => {
                let column_names = if let Ok(names) = take_argument(&mut arguments, "names") {
                    dimensionality = names.ref_array()?.shape().len() + 1;
                    let mut indices = match names.array()? {
                        Array::Str(names) => to_name_vec(names)?
                            .into_iter().map(IndexKey::from).collect(),
                        Array::Int(names) => to_name_vec(names)?
                            .into_iter().map(IndexKey::from).collect(),
                        Array::Bool(names) => to_name_vec(names)?
                            .into_iter().map(IndexKey::from).collect(),
                        Array::Float(_) => return Err("cannot index by floats".into()),
                    };
                    if is_partition && dimensionality == 2 {
                        indices = vec![IndexKey::Tuple(indices)]
                    }
                    indices

                } else if let Ok(indices) = take_argument(&mut arguments, "indices") {
                    dimensionality = indices.ref_array()?.shape().len() + 1;
                    let column_names = dataframe.keys().cloned().collect::<Vec<IndexKey>>();
                    to_name_vec(indices.array()?.int()?)?.iter()
                        .map(|index| column_names.get(*index as usize).cloned()
                            .ok_or_else(|| Error::from("column index out of bounds"))).collect::<Result<Vec<IndexKey>>>()?

                } else if let Ok(mask) = take_argument(&mut arguments, "mask") {
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
                let indices = if let Ok(indices) = take_argument(&mut arguments, "indices") {
                    let indices = indices.array()?.int()?;
                    dimensionality = indices.shape().len() + 1;
                    to_name_vec(indices)?.into_iter()
                        .map(|v| v as usize).collect()
                } else if let Ok(mask) = take_argument(&mut arguments, "mask") {
                    dimensionality = 2;
                    to_name_vec(mask.array()?.bool()?)?.into_iter().enumerate()
                        .filter(|(_, mask)| *mask)
                        .map(|(idx, _)| idx)
                        .collect::<Vec<usize>>()
                } else {
                    return Err("indices or mask must be supplied when indexing on arrays".into())
                };
                Ok(match array {
                    Array::Int(data) => data.select(Axis(1), &indices).into(),
                    Array::Float(data) => data.select(Axis(1), &indices).into(),
                    Array::Bool(data) => data.select(Axis(1), &indices).into(),
                    Array::Str(data) => slow_select(&data, Axis(1), &indices).into(),
                })
            }
            Value::Jagged(_) => return Err("indexing is not supported for jagged arrays".into()),
            Value::Function(_) => return Err("indexing is not supported for functions".into())
        }?;

        // remove trailing singleton axis if a zero-dimensional index set was passed
        if let Value::Array(array) = &mut indexed {
            if !is_partition && dimensionality == 1 && array.shape().len() == 2 {
                match array {
                    Array::Float(array) => array.index_axis_inplace(Axis(1), 0),
                    Array::Int(array) => array.index_axis_inplace(Axis(1), 0),
                    Array::Bool(array) => array.index_axis_inplace(Axis(1), 0),
                    Array::Str(array) => array.index_axis_inplace(Axis(1), 0),
                }
            }
        };

        Ok(ReleaseNode::new(indexed))
    }
}

pub fn mask_columns(column_names: &[IndexKey], mask: &[bool]) -> Result<Vec<IndexKey>> {
    if mask.len() != column_names.len() {
        return Err("boolean mask must be the same length as the column names".into());
    }
    Ok(column_names.iter().zip(mask)
        .filter(|(_, mask)| **mask)
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<IndexKey>>())
}

fn column_stack(
    mut dataframe: IndexMap<IndexKey, Value>,
    column_names: &[IndexKey],
) -> Result<Value> {
    if column_names.len() == 1 {
        let column_name = column_names.first().unwrap();
        return dataframe.remove(column_name)
            .ok_or_else(|| Error::from(format!("the provided column does not exist: {:?}", column_name)));
    }

    fn to_2d<T>(array: ArrayD<T>) -> Result<ArrayD<T>> {
        to_nd(array, 2)
    }

    let values = column_names.iter()
        .map(|column_name| dataframe.get(column_name)
            .ok_or_else(|| Error::from(format!("one of the provided column names does not exist: {:?}", column_name))))
        .collect::<Result<Vec<&Value>>>()?;

    let data_type = match values.first() {
        Some(value) => match value.ref_array()? {
            Array::Float(_) => DataType::Float,
            Array::Int(_) => DataType::Int,
            Array::Bool(_) => DataType::Bool,
            Array::Str(_) => DataType::Str,
        },
        None => return Err("at least one column must be supplied to Index".into())
    };

    match data_type {
        DataType::Unknown => unreachable!(),
        DataType::Float => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.remove(column_name)
                    .ok_or_else(|| Error::from(format!("one of the provided column names does not exist: {:?}", column_name)))
                    .and_then(|array| to_2d(array.array()?.float()?)))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Int => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.remove(column_name)
                    .ok_or_else(|| Error::from(format!("one of the provided column names does not exist: {:?}", column_name)))
                    .and_then(|array| to_2d(array.array()?.int()?)))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Bool => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.remove(column_name)
                    .ok_or_else(|| Error::from(format!("one of the provided column names does not exist: {:?}", column_name)))
                    .and_then(|array| to_2d(array.array()?.bool()?)))
                .collect::<Result<Vec<_>>>()?;

            Ok(ndarray::stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<_>>>())?.into())
        }
        DataType::Str => {
            let chunks = column_names.iter()
                .map(|column_name| dataframe.remove(column_name)
                    .ok_or_else(|| Error::from(format!("one of the provided column names does not exist: {:?}", column_name)))
                    .and_then(|array| to_2d(array.array()?.string()?)))
                .collect::<Result<Vec<ArrayD<String>>>>()?;

            Ok(slow_stack(Axis(1), &chunks.iter()
                .map(|chunk| chunk.view()).collect::<Vec<ArrayViewD<String>>>())?.into())
        }
    }
}
