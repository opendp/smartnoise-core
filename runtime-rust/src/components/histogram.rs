use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use crate::components::Evaluable;
use ndarray::ArrayD;
use ndarray;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::get_argument;
use std::collections::BTreeMap;
use crate::utilities::get_num_columns;
use noisy_float::types::n64;


impl Evaluable for proto::Histogram {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match (get_argument(arguments, "data")?.array()?, get_argument(arguments, "categories")?.array()?) {
            (Array::Bool(data), Array::Bool(categories)) =>
                histogram(data, categories)?.into(),
            (Array::F64(data), Array::F64(categories)) =>
                histogram(&data.mapv(n64), &categories.mapv(n64))?.into(),
            (Array::I64(data), Array::I64(categories)) =>
                histogram(data, categories)?.into(),
            (Array::Str(data), Array::Str(categories)) =>
                histogram(data, categories)?.into(),
            _ => return Err("data and categories must be homogeneously typed".into())
        }))
    }
}

pub fn histogram<T: Clone + Eq + Ord + std::hash::Hash>(data: &ArrayD<T>, categories: &ArrayD<T>) -> Result<ArrayD<i64>> {
    let zeros = categories.iter()
        .map(|cat| (cat, 0)).collect::<BTreeMap<&T, i64>>();

    let counts = data.gencolumns().into_iter()
        .map(|column| {
            let mut counts = zeros.clone();
            column.into_iter().for_each(|v| {
                counts.entry(v).and_modify(|v| *v += 1);
            });
            categories.iter()
                .map(|cat| counts.get(cat).unwrap())
                .cloned().collect::<Vec<i64>>()
        }).flat_map(|v| v).collect::<Vec<i64>>();

    // ensure histogram is of correct dimension
    Ok(match data.ndim() {
        1 => ndarray::Array::from_shape_vec(vec![zeros.len()], counts),
        2 => ndarray::Array::from_shape_vec(vec![zeros.len(), get_num_columns(&data)? as usize], counts),
        _ => return Err("invalid data shape for Histogram".into())
    }?.into())
}