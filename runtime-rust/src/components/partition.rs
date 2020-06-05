use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Value, Jagged, IndexKey};
use whitenoise_validator::utilities::get_argument;
use whitenoise_validator::components::partition::even_split_lengths;
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis};

use whitenoise_validator::proto;

use whitenoise_validator::utilities::array::slow_select;
use indexmap::map::IndexMap;
use std::hash::Hash;


impl Evaluable for proto::Partition {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;
        Ok(ReleaseNode::new(match arguments.get::<IndexKey>(&"by".into()) {
            Some(by) => match (by.array()?, get_argument(arguments, "categories")?.jagged()?) {
                (Array::I64(by), Jagged::I64(categories)) =>
                    Value::Indexmap(partition_by(data, &by, categories.get(0).ok_or_else(|| "categories may not be empty")?)?
                        .into_iter().map(|(k, v)| (IndexKey::from(k), v)).collect()),
                (Array::Bool(by), Jagged::Bool(categories)) =>
                    Value::Indexmap(partition_by(data, &by, categories.get(0).ok_or_else(|| "categories may not be empty")?)?
                        .into_iter().map(|(k, v)| (IndexKey::from(k), v)).collect()),
                (Array::Str(by), Jagged::Str(categories)) =>
                    Value::Indexmap(partition_by(data, &by, categories.get(0).ok_or_else(|| "categories may not be empty")?)?
                        .into_iter().map(|(k, v)| (IndexKey::from(k), v)).collect()),
                _ => return Err("by and categories must share the same type".into())
            },
            None => {
                let num_partitions = get_argument(arguments, "num_partitions")?
                    .array()?.first_i64()?;

                match data {
                    Array::F64(data) =>
                        Value::Indexmap(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>()),
                    Array::I64(data) =>
                        Value::Indexmap(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>()),
                    Array::Bool(data) =>
                        Value::Indexmap(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>()),
                    Array::Str(data) =>
                        Value::Indexmap(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>()),
                }
            }
        }))
    }
}

/// Partitions data evenly into num_partitions partitions
///
/// The first partitions may have one more element than the latter partitions.
///
/// # Arguments
/// * `data` - Data to be partitioned.
/// * `num_partitions` - Number of keys in the indexmap of arrays returned.
///
/// # Return
/// Indexmap with data splits.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::partition::partition_evenly;
///
/// let data = arr2(&[ [1, 2], [4, 5], [7, 8], [10, 11] ]).into_dyn();
/// let partitioned = partition_evenly(&data, 3);
/// assert_eq!(partitioned.get(&0).unwrap().clone(), arr2(&[ [1, 2], [4, 5] ]).into_dyn());
/// assert_eq!(partitioned.get(&1).unwrap().clone(), arr2(&[ [7, 8] ]).into_dyn());
/// assert_eq!(partitioned.get(&2).unwrap().clone(), arr2(&[ [10, 11] ]).into_dyn());
/// ```
pub fn partition_evenly<T: Clone + Default + std::fmt::Debug>(
    data: &ArrayD<T>, num_partitions: i64
) -> IndexMap<IndexKey, ArrayD<T>> {

    let mut offset = 0;
    even_split_lengths(data.len_of(Axis(0)) as i64, num_partitions).into_iter().enumerate()
        .map(|(idx, length)| {

            let entry = (
                IndexKey::from(idx as i64),
                slow_select(data, Axis(0),
                            &(offset as usize..(offset + length) as usize).collect::<Vec<usize>>())
            );
            offset += length;
            entry
        })
        .collect::<IndexMap<IndexKey, ArrayD<T>>>()
}

pub fn partition_by<T: Clone + Hash + Eq>(
    data: &Array, by: &ArrayD<T>, categories: &Vec<T>
) -> Result<IndexMap<T, Value>> {
    let mut indices = categories.iter()
        .map(|cat| (cat.clone(), vec![]))
        .collect::<IndexMap<T, Vec<usize>>>();

    by.clone()
        .into_dimensionality::<ndarray::Ix1>()?.iter().enumerate()
        .for_each(|(idx, cat)| indices.entry(cat.clone())
            .or_insert_with(Vec::new).push(idx));

    Ok(match data {
        Array::I64(data) => indices.into_iter()
            .map(|(cat, idxs)| (cat, data.select(ndarray::Axis(0), &idxs).into()))
            .collect::<IndexMap<T, Value>>(),
        Array::F64(data) => indices.into_iter()
            .map(|(cat, idxs)| (cat, data.select(ndarray::Axis(0), &idxs).into()))
            .collect::<IndexMap<T, Value>>(),
        Array::Bool(data) => indices.into_iter()
            .map(|(cat, idxs)| (cat, data.select(ndarray::Axis(0), &idxs).into()))
            .collect::<IndexMap<T, Value>>(),
        Array::Str(data) => indices.into_iter()
            .map(|(cat, idxs)| (cat, slow_select(data, ndarray::Axis(0), &idxs).into()))
            .collect::<IndexMap<T, Value>>()
    })
}