use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Value};
use crate::components::Evaluable;
use ndarray::ArrayD;

use whitenoise_validator::{proto, Integer};
use whitenoise_validator::utilities::take_argument;
use crate::utilities::get_num_columns;
use noisy_float::types::n64;
use indexmap::{indexmap, IndexMap};
use std::collections::HashMap;


impl Evaluable for proto::Histogram {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?;
        Ok(ReleaseNode::new(match take_argument(&mut arguments, "categories")?.array() {
            Ok(categories) => match (data, categories) {
                (Array::Bool(data), Array::Bool(categories)) =>
                    histogram_categories(&data, &categories)?.into(),
                (Array::Float(data), Array::Float(categories)) =>
                    histogram_categories(&data.mapv(|v| n64(v as f64)), &categories.mapv(|v| n64(v as f64)))?.into(),
                (Array::Int(data), Array::Int(categories)) =>
                    histogram_categories(&data, &categories)?.into(),
                (Array::Str(data), Array::Str(categories)) =>
                    histogram_categories(&data, &categories)?.into(),
                _ => return Err("data and categories must be homogeneously typed".into())
            }
            Err(_) => match data {
                Array::Str(data) => {
                    let (labels, counts) = histogram(data)?;
                    Value::Dataframe(indexmap![
                        "categories".into() => Value::Array(Array::Str(labels)),
                        "counts".into() => Value::Array(Array::Int(counts))])
                }
                Array::Int(data) => {
                    let (labels, counts) = histogram(data)?;
                    Value::Dataframe(indexmap![
                        "categories".into() => Value::Array(Array::Int(labels)),
                        "counts".into() => Value::Array(Array::Int(counts))])
                }
                _ => return Err("stability histograms are only supported for strings and integers".into())
            }
        }))
    }
}

pub fn histogram<T: Clone + Eq + Ord + std::hash::Hash>(
    data: ArrayD<T>
) -> Result<(ArrayD<T>, ArrayD<Integer>)> {
    let mut counts = HashMap::new();
    let data = data.into_dimensionality::<ndarray::Ix1>()?;

    data.iter().for_each(|v| {
        *counts.entry(v).or_insert(0) += 1;
    });

    let (labels, counts): (Vec<&T>, Vec<Integer>) = counts.into_iter().unzip();

    Ok((
        ndarray::arr1(&labels.into_iter().cloned().collect::<Vec<_>>()).into_dyn(),
        ndarray::arr1(&counts).into_dyn()
    ))
}

pub fn histogram_categories<T: Clone + Eq + Ord + std::hash::Hash>(
    data: &ArrayD<T>, categories: &ArrayD<T>
) -> Result<ArrayD<Integer>> {
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