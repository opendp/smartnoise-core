use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode, Jagged, Value, IndexKey};
use whitenoise_validator::utilities::take_argument;
use crate::components::Evaluable;
use whitenoise_validator::{proto, Float};
use ndarray::{ArrayD, Axis};

use ndarray_stats::QuantileExt;
use ndarray_stats::interpolate;
use noisy_float::types::n64;
use num::{FromPrimitive, ToPrimitive};
use std::ops::{Sub, Div, Add, Mul, Rem};


impl Evaluable for proto::Quantile {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?;

        Ok(match arguments.remove::<IndexKey>(&"candidates".into()) {
            Some(candidates) => match (candidates.jagged()?, data) {
                (Jagged::Float(candidates), Array::Float(data)) => Value::Jagged(quantile_utilities(
                    candidates.iter().map(|col| col.iter().copied().map(|v| n64(v as f64)).collect()).collect(),
                    &data.mapv(|v| n64(v as f64)),
                    self.alpha as Float)?.into()),
                (Jagged::Int(candidates), Array::Int(data)) => Value::Jagged(quantile_utilities(
                    candidates,
                    &data,
                    self.alpha as Float)?.into()),
                _ => return Err("data must be either f64 or i64".into())
            },
            None => match data {
                Array::Float(data) =>
                    quantile(data.mapv(|v| n64(v as f64)), self.alpha, &self.interpolation)?.mapv(|v| v.raw() as Float).into(),
                Array::Int(data) =>
                    quantile(data, self.alpha, &self.interpolation)?.into(),
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
/// use whitenoise_validator::Float;
/// let data: ArrayD<Float> = arr2(&[ [0., 1., 2.], [2., 3., 4.] ]).into_dyn();
/// let median = quantile(data.mapv(|v| n64(v as f64)), 0.5, &"midpoint".to_string()).unwrap();
/// println!("{:?}", median);
/// assert_eq!(median, arr1(& [1.0, 2.0, 3.0] ).into_dyn().mapv(|v| n64(v as f64)));
/// ```
pub fn quantile<T: FromPrimitive + Ord + Clone + Sub<Output=T> + Mul<Output=T> + Div<Output=T> + Add<Output=T> + Rem<Output=T> + ToPrimitive>(
    mut data: ArrayD<T>, alpha: f64, interpolation: &str
) -> Result<ArrayD<T>> {
    if 0. > alpha || alpha > 1. {
        return Err("q must be within [0, 1]".into());
    }

    match match interpolation.to_lowercase().as_str() {
        "lower" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Lower),
        "upper" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Higher),
        "midpoint" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Midpoint),
        "nearest" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Nearest),
        "linear" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Linear),
        _ => return Err(format!("interpolation type not recognized: {}", interpolation).into())
    }  {
        Ok(quantiles) => Ok(quantiles),
        Err(_) => Err("unable to compute quantiles".into())
    }
}


pub fn quantile_utilities<T: Ord + Clone + Copy>(
    candidates: Vec<Vec<T>>,
    data: &ArrayD<T>, alpha: Float
) -> Result<Vec<Vec<Float>>> {
    let n = data.len_of(Axis(0)) as Float;
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
                    offsets.push(offset as Float);
                    index += 1;
                }
            });

            // ensure offsets and candidates have the same length by appending offsets for candidates greater than the maximum value of the dataset
            offsets.extend((0..candidates.len() - offsets.len())
                .map(|_| column_len as Float).collect::<Vec<Float>>());

            let utilities = offsets.into_iter()
                .map(|offset| constant * n - ((1. - alpha) * offset - alpha * (n - offset)).abs())
                .collect::<Vec<Float>>();

            // order they utilities by the order of the candidates before they were sorted
            candidates.into_iter().map(|(idx, _)| utilities[idx]).collect()
        })
        .collect::<Vec<Vec<Float>>>())
}