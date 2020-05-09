use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Value, Indexmap};
use whitenoise_validator::utilities::get_argument;
use whitenoise_validator::components::partition::even_split_lengths;
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis};

use whitenoise_validator::proto;

use whitenoise_validator::utilities::array::slow_select;
use indexmap::map::IndexMap;


impl Evaluable for proto::Partition {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;
        Ok(ReleaseNode::new(match get_argument(arguments, "by") {
            Ok(_value) => return Err("partitioning by categories is not implemented".into()),
            Err(_) => {

                let num_partitions = get_argument(arguments, "num_partitions")?
                    .array()?.first_i64()?;

                match data {
                    Array::F64(data) =>
                        Value::Indexmap(Indexmap::<Value>::I64(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<i64, Value>>())),
                    Array::I64(data) =>
                        Value::Indexmap(Indexmap::<Value>::I64(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<i64, Value>>())),
                    Array::Bool(data) =>
                        Value::Indexmap(Indexmap::<Value>::I64(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<i64, Value>>())),
                    Array::Str(data) =>
                        Value::Indexmap(Indexmap::<Value>::I64(partition_evenly(data, num_partitions).into_iter()
                            .map(|(idx, data)| (idx, data.into())).collect::<IndexMap<i64, Value>>())),
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
pub fn partition_evenly<T: Clone + Default + std::fmt::Debug>(data: &ArrayD<T>, num_partitions: i64) -> IndexMap<i64, ArrayD<T>> {

    let mut offset = 0;
    even_split_lengths(data.len_of(Axis(0)) as i64, num_partitions).into_iter().enumerate()
        .map(|(idx, length)| {

            let entry = (
                idx as i64,
                slow_select(data, Axis(0),
                            &(offset as usize..(offset + length) as usize).collect::<Vec<usize>>())
            );
            offset += length;
            entry
        })
        .collect::<IndexMap<i64, ArrayD<T>>>()
}