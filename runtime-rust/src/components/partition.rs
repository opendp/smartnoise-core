use yarrow_validator::errors::*;

use crate::components::Evaluable;
use crate::base::NodeArguments;
use yarrow_validator::base::{Value, get_argument, ArrayND, Hashmap};

use ndarray::prelude::*;
use crate::utilities::array::select;
use std::collections::HashMap;
use yarrow_validator::proto;
use crate::utilities::noise::sample_uniform;

extern crate num;
use num::Integer;
use std::hash::Hash;


impl Evaluable for proto::Partition {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(arguments, "data")?.get_arraynd()?;

        match (
            get_argument(arguments, "partitions"),
            get_argument(arguments, "by"),
            get_argument(arguments, "categories")
        ) {
            // partition by some given data column
            (Ok(partitions), Ok(by), Ok(categories)) => match (by.get_arraynd()?, categories.get_arraynd()?) {
                (ArrayND::Bool(by), ArrayND::Bool(categories)) =>
                    Ok(omni_partition(data, &Hashmap::<Vec<usize>>::Bool(reindex_by(&by, &categories)?))),
                (ArrayND::I64(by), ArrayND::I64(categories)) =>
                    Ok(omni_partition(data, &Hashmap::<Vec<usize>>::I64(reindex_by(&by, &categories)?))),
                (ArrayND::Str(by), ArrayND::Str(categories)) =>
                    Ok(omni_partition(data, &Hashmap::<Vec<usize>>::Str(reindex_by(&by, &categories)?))),
                _ => return Err("Partition by floats is not supported".into())
            },

            // partition equally
            (Ok(partitions), Err(_), Err(_)) =>
                Ok(omni_partition(data, &Hashmap::<Vec<usize>>::I64(reindex_equally(&data.get_num_records()?, &partitions.get_first_i64()?)?)).into()),
            _ => Err("Partition requires 'data', and optionally 'by' clamped to categories".into())
        }
    }
}

pub fn reindex_by<T: Clone + Eq + Hash>(by: &ArrayD<T>, categories: &ArrayD<T>) -> Result<HashMap<T, Vec<usize>>> {
    let by = by.gencolumns().into_iter().next()
        .ok_or::<Error>("'by' must have at least one column".into())?;

    Ok(categories.iter()
        .map(|category|
            (category.clone(), by.iter().enumerate()
                .filter(|(_, v)| *v == category)
                .map(|(i, _)| i)
                .collect()))
        .collect())
}

pub fn reindex_equally(length: &i64, partitions: &i64) -> Result<HashMap<i64, Vec<usize>>> {
    let mut indices: Vec<_> = (0..*length)
        .map(|index| Ok((index as usize, sample_uniform(&0., &1.)?)))
        .collect::<Result<Vec<(usize, f64)>>>()?;
    indices.sort_unstable_by(|l, r| l.1.partial_cmp(&r.1).unwrap());

    let (floor, remainder) = length.div_mod_floor(partitions);
    Ok((0..*partitions).map(|i|
        (i as i64, indices[(i * floor + i.min(remainder)) as usize..((i + 1) * floor + (i + 1).min(remainder)) as usize]
            .iter().map(|v| v.0).collect())
    ).collect())
}

pub fn omni_partition(data: &ArrayND, reindex: &Hashmap<Vec<usize>>) -> Value {
    match data {
        ArrayND::Bool(data) => match reindex {
            Hashmap::Bool(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<bool, Value>>().into(),
            Hashmap::I64(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<i64, Value>>().into(),
            Hashmap::Str(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<String, Value>>().into(),
        },
        ArrayND::I64(data) => match reindex {
            Hashmap::Bool(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<bool, Value>>().into(),
            Hashmap::I64(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<i64, Value>>().into(),
            Hashmap::Str(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<String, Value>>().into(),
        },
        ArrayND::Str(data) => match reindex {
            Hashmap::Bool(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<bool, Value>>().into(),
            Hashmap::I64(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<i64, Value>>().into(),
            Hashmap::Str(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<String, Value>>().into(),
        },
        ArrayND::F64(data) => match reindex {
            Hashmap::Bool(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<bool, Value>>().into(),
            Hashmap::I64(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<i64, Value>>().into(),
            Hashmap::Str(reindex) => partition(data, reindex).into_iter()
                .map(|(k, v)| (k, v.into())).collect::<HashMap<String, Value>>().into(),
        },
    }
}

pub fn partition<T: Clone, U: Clone + Eq + Hash>(data: &ArrayD<T>, reindex: &HashMap<U, Vec<usize>>) -> HashMap<U, ArrayD<T>> {
    reindex.iter()
        .map(|(partition_name, indices)|
            (partition_name.clone(), select(data, Axis(0), indices).clone()))
        .collect()
}
