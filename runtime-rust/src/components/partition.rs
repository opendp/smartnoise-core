use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Value, IndexKey};
use whitenoise_validator::utilities::{take_argument, get_common_value};
use whitenoise_validator::components::partition::{even_split_lengths, make_dense_partition_keys};
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis};

use whitenoise_validator::{proto, Integer};

use whitenoise_validator::utilities::array::slow_select;
use indexmap::map::IndexMap;


impl Evaluable for proto::Partition {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?;
        Ok(ReleaseNode::new(match arguments.remove::<IndexKey>(&"by".into()) {
            Some(by) => {
                let categories = take_argument(&mut arguments, "categories")?.jagged()?;
                let partitions = make_dense_partition_keys(
                    categories, Some(by.ref_array()?.shape().len() as i64))?;

                match by.array()? {
                    Array::Int(by) =>
                        Value::Indexmap(partition_by(&data, by.mapv(IndexKey::from), partitions)?),
                    Array::Bool(by) =>
                        Value::Indexmap(partition_by(&data, by.mapv(IndexKey::from), partitions)?),
                    Array::Str(by) =>
                        Value::Indexmap(partition_by(&data, by.mapv(IndexKey::from), partitions)?),
                    _ => return Err("by and categories must share the same type".into())
                }
            },
            None => {
                let num_partitions = take_argument(&mut arguments, "num_partitions")?
                    .array()?.first_int()?;

                Value::Indexmap(partition_evenly(&data, num_partitions as i64)?)
            }
        }))
    }
}

// to make the nested indexmaps more readable
type ColName = IndexKey;

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
/// use whitenoise_runtime::components::partition::partition_ndarray_evenly;
/// use whitenoise_validator::base::IndexKey;
///
/// let data = arr2(&[ [1, 2], [4, 5], [7, 8], [10, 11] ]).into_dyn();
/// let partitioned = partition_ndarray_evenly(&data, 3);
/// assert_eq!(partitioned.get::<IndexKey>(&0.into()).unwrap().clone(), arr2(&[ [1, 2], [4, 5] ]).into_dyn());
/// assert_eq!(partitioned.get::<IndexKey>(&1.into()).unwrap().clone(), arr2(&[ [7, 8] ]).into_dyn());
/// assert_eq!(partitioned.get::<IndexKey>(&2.into()).unwrap().clone(), arr2(&[ [10, 11] ]).into_dyn());
/// ```
pub fn partition_ndarray_evenly<T: Clone + Default + std::fmt::Debug>(
    data: &ArrayD<T>, num_partitions: i64
) -> IndexMap<IndexKey, ArrayD<T>> {

    let mut offset = 0;
    even_split_lengths(data.len_of(Axis(0)) as i64, num_partitions).into_iter().enumerate()
        .map(|(idx, length)| {

            let entry = (
                IndexKey::from(idx as Integer),
                slow_select(data, Axis(0),
                            &(offset as usize..(offset + length) as usize).collect::<Vec<usize>>())
            );
            offset += length;
            entry
        })
        .collect::<IndexMap<IndexKey, ArrayD<T>>>()
}

pub fn partition_evenly(data: &Value, num_partitions: i64) -> Result<IndexMap<IndexKey, Value>> {
    Ok(match data {
        Value::Indexmap(data) => {

            let columnar_partitions = data.into_iter()
                .map(|(k, v)| Ok((
                    k.clone(),
                    partition_evenly(v, num_partitions)?
                )))
                .collect::<Result<IndexMap<ColName, IndexMap<IndexKey, Value>>>>()?;

            let number_rows: usize = get_common_value(&data.values()
                .map(|v| v.ref_array()?.num_records())
                .collect::<Result<Vec<usize>>>()?)
                .ok_or_else(|| Error::from("columns of a dataframe must share the same length"))?;

            even_split_lengths(number_rows as i64, num_partitions).into_iter()
                .map(|idx| IndexKey::from(idx as Integer))
                .map(|idx| (
                    idx.clone(),
                    Value::Indexmap(columnar_partitions.iter().map(|(colname, partitions)|
                        (colname.clone(), partitions.get(&idx).unwrap().clone())
                    ).collect::<IndexMap<ColName, Value>>())
                ))
                .collect::<IndexMap<IndexKey, Value>>()
        },
        Value::Array(data) => match data {
            Array::Float(data) =>
                partition_ndarray_evenly(data, num_partitions).into_iter()
                    .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>(),
            Array::Int(data) =>
                partition_ndarray_evenly(data, num_partitions).into_iter()
                    .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>(),
            Array::Bool(data) =>
                partition_ndarray_evenly(data, num_partitions).into_iter()
                    .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>(),
            Array::Str(data) =>
                partition_ndarray_evenly(data, num_partitions).into_iter()
                    .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<IndexKey, Value>>(),
        },
        _ => return Err("data: must be a dataframe or array".into())
    })

}

pub fn partition_by(
    data: &Value, by: ArrayD<IndexKey>, partition_keys: Vec<IndexKey>
) -> Result<IndexMap<IndexKey, Value>> {

    let mut indices = partition_keys.into_iter()
        .map(|key| (key, vec![]))
        .collect::<IndexMap<IndexKey, Vec<usize>>>();

    match by.ndim() {
        0 => return Err("by: invalid dimensionality".into()),
        1 => by.into_dimensionality::<ndarray::Ix1>()?,
        _ => by.genrows().into_iter().map(|row| IndexKey::Tuple(row.to_vec())).collect()
    }
        .into_iter().enumerate()
        .for_each(|(idx, cat)| indices.entry(cat.clone())
            .or_insert_with(Vec::new).push(idx));

    // partition either an array or a dataframe
    fn value_partitioner(data: &Value, indices: &IndexMap<IndexKey, Vec<usize>>) -> Result<IndexMap<IndexKey, Value>> {
        Ok(match data {
            Value::Array(data) => match data {
                Array::Int(data) => indices.into_iter()
                    .map(|(cat, idxs)| (cat.clone(), data.select(ndarray::Axis(0), idxs).into()))
                    .collect::<IndexMap<IndexKey, Value>>(),
                Array::Float(data) => indices.into_iter()
                    .map(|(cat, idxs)| (cat.clone(), data.select(ndarray::Axis(0), idxs).into()))
                    .collect::<IndexMap<IndexKey, Value>>(),
                Array::Bool(data) => indices.into_iter()
                    .map(|(cat, idxs)| (cat.clone(), data.select(ndarray::Axis(0), idxs).into()))
                    .collect::<IndexMap<IndexKey, Value>>(),
                Array::Str(data) => indices.into_iter()
                    .map(|(cat, idxs)| (cat.clone(), slow_select(&data, ndarray::Axis(0), idxs).into()))
                    .collect::<IndexMap<IndexKey, Value>>()
            },

            Value::Indexmap(data) => {
                let columnar_partitions = data.into_iter().map(|(k, v)|
                    Ok((k.clone(), value_partitioner(v, indices)?)))
                    .collect::<Result<IndexMap<ColName, IndexMap<IndexKey, Value>>>>()?;

                indices.iter()
                    .map(|(cat, _)| (
                        cat.clone(),
                        Value::Indexmap(columnar_partitions.iter().map(|(colname, partitions)|
                            (colname.clone(), partitions.get(&cat.clone()).unwrap().clone())
                        ).collect::<IndexMap<ColName, Value>>())
                    ))
                    .collect::<IndexMap<IndexKey, Value>>()
            },
            _ => return Err("data: must be a dataframe or array".into())
        })
    };

    value_partitioner(data, &indices)
}