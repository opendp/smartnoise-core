use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Jagged, Value};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use whitenoise_validator::proto;
use ndarray::{ArrayD, Axis};

use ndarray_stats::QuantileExt;
use ndarray_stats::interpolate;
use noisy_float::types::n64;
use num::{FromPrimitive, ToPrimitive};
use std::ops::{Sub, Div, Add, Mul, Rem};


impl Evaluable for proto::Quantile {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;

        Ok(match arguments.get("candidates") {
            Some(candidates) => match (candidates.jagged()?, data) {
                (Jagged::F64(candidates), Array::F64(data)) => Value::Jagged(quantile_utilities(
                    candidates.into_iter().map(|col| col.into_iter().copied().map(n64).collect()).collect(),
                    &data.mapv(n64),
                    &self.alpha)?.into()),
                (Jagged::I64(candidates), Array::I64(data)) => Value::Jagged(quantile_utilities(
                    candidates.clone(),
                    data,
                    &self.alpha)?.into()),
                _ => return Err("data must be either f64 or i64".into())
            },
            None => match data {
                Array::F64(data) =>
                    quantile(data.mapv(n64), &self.alpha, &self.interpolation)?.mapv(|v| v.raw()).into(),
                Array::I64(data) =>
                    quantile(data.clone(), &self.alpha, &self.interpolation)?.into(),
                _ => return Err("data must be either f64 or i64".into())
            }
        }).map(ReleaseNode::new)
    }
}


/// Accepts data and returns desired quantile of each column in the data.
///
/// # Arguments
/// * `data` - Array of data for which you would like the quantile.
/// * `alpha` - Desired quantile.
///
/// # Return
/// Quantile of interest for each column of your data.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::quantile::quantile;
/// use noisy_float::types::n64;
/// let data: ArrayD<f64> = arr2(&[ [0., 1., 2.], [2., 3., 4.] ]).into_dyn();
/// let median = quantile(data.mapv(n64), &0.5, &"midpoint".to_string()).unwrap();
/// println!("{:?}", median);
/// assert_eq!(median, arr1(& [1.0, 2.0, 3.0] ).into_dyn().mapv(n64));
/// ```
pub fn quantile<T: FromPrimitive + Ord + Clone + Sub<Output=T> + Mul<Output=T> + Div<Output=T> + Add<Output=T> + Rem<Output=T> + ToPrimitive>(
    mut data: ArrayD<T>, alpha: &f64, interpolation: &String
) -> Result<ArrayD<T>> {
    if &0. > alpha || alpha > &1. {
        return Err("q must be within [0, 1]".into());
    }

    match match interpolation.to_lowercase().as_str() {
        "lower" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Lower),
        "upper" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Higher),
        "midpoint" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Midpoint),
        "nearest" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Nearest),
        "linear" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Linear),
        _ => return Err(format!("interpolation type not recognized: {}", interpolation).into())
    }  {
        Ok(quantiles) => Ok(quantiles),
        Err(_) => Err("unable to compute quantiles".into())
    }
}


pub fn quantile_utilities<T: Ord + Clone + Copy>(
    candidates: Vec<Vec<T>>,
    data: &ArrayD<T>, alpha: &f64
) -> Result<Vec<Vec<f64>>> {
    let n = data.len_of(Axis(0)) as f64;
    let constant = alpha.max(1. - alpha);

    Ok(data.gencolumns().into_iter().zip(candidates.into_iter())
        .map(|(column, candidates)| {
            let mut column = column.to_vec();
            let column_len = column.len();
            let mut candidates = candidates.into_iter().enumerate().collect::<Vec<(usize, T)>>();
            candidates.sort_unstable_by_key(|v| v.1);
            column.sort_unstable();

            let mut offsets = Vec::new();
            let mut index = 0;
            column.into_iter().enumerate().for_each(|(offset, v)| {
                while index < candidates.len() && v > candidates[index].1 {
                    offsets.push(offset as f64);
                    index += 1;
                }
            });

            // ensure offsets and candidates have the same length by appending offsets for candidates greater than the maximum value of the dataset
            offsets.extend((0..candidates.len() - offsets.len())
                .map(|_| column_len as f64).collect::<Vec<f64>>());

            let utilities = offsets.into_iter()
                .map(|offset| constant - ((1. - alpha) * offset - alpha * (n - offset)).abs())
                .collect::<Vec<f64>>();

            // order they utilities by the order of the candidates before they were sorted
            candidates.into_iter().map(|(idx, _)| utilities[idx]).collect()
        })
        .collect::<Vec<Vec<f64>>>())
}