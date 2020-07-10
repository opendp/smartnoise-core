use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use crate::components::Evaluable;
use ndarray::ArrayD;

use whitenoise_validator::{proto, Integer};
use whitenoise_validator::utilities::take_argument;
use crate::utilities::get_num_columns;
use noisy_float::types::n64;
use indexmap::map::IndexMap;


impl Evaluable for proto::Histogram {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match (take_argument(&mut arguments, "data")?.array()?, take_argument(&mut arguments, "categories")?.array()?) {
            (Array::Bool(data), Array::Bool(categories)) =>
                histogram(&data, &categories)?.into(),
            (Array::Float(data), Array::Float(categories)) =>
                histogram(&data.mapv(|v| n64(v as f64)), &categories.mapv(|v| n64(v as f64)))?.into(),
            (Array::Int(data), Array::Int(categories)) =>
                histogram(&data, &categories)?.into(),
            (Array::Str(data), Array::Str(categories)) =>
                histogram(&data, &categories)?.into(),
            _ => return Err("data and categories must be homogeneously typed".into())
        }))
    }
}

pub fn histogram<T: Clone + Eq + Ord + std::hash::Hash>(
    data: &ArrayD<T>, categories: &ArrayD<T>) -> Result<ArrayD<Integer>> {
    let zeros = categories.iter()
        .map(|cat| (cat, 0)).collect::<IndexMap<&T, Integer>>();

    let counts = data.gencolumns().into_iter()
        .map(|column| {
            let mut counts = zeros.clone();
            column.into_iter().for_each(|v| {
                counts.entry(v).and_modify(|v| *v += 1);
            });
            categories.iter()
                .map(|cat| counts.get(cat).unwrap())
                .cloned().collect::<Vec<Integer>>()
        }).flatten().collect::<Vec<Integer>>();

    // ensure histogram is of correct dimension
    Ok(match data.ndim() {
        1 => ndarray::Array::from_shape_vec(vec![zeros.len()], counts),
        2 => ndarray::Array::from_shape_vec(vec![zeros.len(), get_num_columns(&data)? as usize], counts),
        _ => return Err("invalid data shape for Histogram".into())
    }?)
}