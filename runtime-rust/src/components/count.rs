use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array, Axis};
use whitenoise_validator::proto;
use whitenoise_validator::utilities::get_argument;
use std::collections::BTreeMap;
use crate::utilities::utilities::get_num_columns;
use noisy_float::types::n64;


impl Evaluable for proto::Count {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        Ok(match (get_argument(&arguments, "data"), get_argument(&arguments, "categories")) {
            (Ok(data), Ok(categories)) => match (data.get_arraynd()?, categories.get_arraynd()?) {
                (ArrayND::Bool(data), ArrayND::Bool(categories)) =>
                    count_by(data, categories)?.into(),
                (ArrayND::F64(data), ArrayND::F64(categories)) =>
                    count_by(&data.mapv(n64), &categories.mapv(n64))?.into(),
                (ArrayND::I64(data), ArrayND::I64(categories)) =>
                    count_by(data, categories)?.into(),
                (ArrayND::Str(data), ArrayND::Str(categories)) =>
                    count_by(data, categories)?.into(),
                _ => return Err("data and categories must be homogeneously typed".into())
            },
            (Ok(data), ..) => match data.get_arraynd()? {
                ArrayND::Bool(data) => count(data)?.into(),
                ArrayND::F64(data) => count(data)?.into(),
                ArrayND::I64(data) => count(data)?.into(),
                ArrayND::Str(data) => count(data)?.into()
            }
            _ => return Err("data and optionally categories must be passed".into())
        })
    }
}

/// Gets number of rows of data.
///
/// # Arguments
/// * `data` - Data for which you want a count.
///
/// # Return
/// Number of rows in data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::count::count;
/// let data = arr2(&[ [false, false, true], [true, true, true] ]).into_dyn();
/// let n = count(&data).unwrap();
/// assert!(n.first().unwrap() == &2);
/// ```
pub fn count<T>(data: &ArrayD<T>) -> Result<ArrayD<i64>> {
    Ok(Array::from_shape_vec(vec![], vec![data.len_of(Axis(0)) as i64])?)
}

pub fn count_by<T: Clone + Eq + Ord + std::hash::Hash>(data: &ArrayD<T>, categories: &ArrayD<T>) -> Result<ArrayD<i64>> {

    let zeros = categories.iter()
        .map(|cat| (cat, 0)).collect::<BTreeMap<&T, i64>>();

    let counts = data.gencolumns().into_iter()
        .map(|column| {
            let mut counts = zeros.clone();
            column.into_iter().for_each(|v| {
                counts.entry(v).and_modify(|v| *v += 1);
            });
            counts.values().cloned().collect::<Vec<i64>>()
        }).flat_map(|v| v).collect::<Vec<i64>>();

    // ensure means are of correct dimension
    Ok(match data.ndim() {
        1 => Array::from_shape_vec(vec![zeros.len()], counts),
        2 => Array::from_shape_vec(vec![zeros.len(), get_num_columns(&data)? as usize], counts),
        _ => return Err("invalid data shape for Count".into())
    }?.into())
}