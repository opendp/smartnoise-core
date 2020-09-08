use std::ops::{Add, Div, Mul, Rem, Sub};

use ndarray::{ArrayD, Axis};
use ndarray_stats::interpolate;
use ndarray_stats::QuantileExt;
use noisy_float::types::n64;
use num::{FromPrimitive, ToPrimitive};

use whitenoise_validator::{Float, proto};
use whitenoise_validator::base::{Array, IndexKey, ReleaseNode, Value};
use whitenoise_validator::errors::*;
use whitenoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;

impl Evaluable for proto::Quantile {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?;

        Ok(match arguments.remove::<IndexKey>(&"candidates".into()) {
            Some(candidates) => {
                let lower = arguments.remove(&IndexKey::from("lower"));
                let upper = arguments.remove(&IndexKey::from("upper"));

                match (candidates.array()?, data) {
                    (Array::Float(candidates), Array::Float(data)) =>
                        Value::Array(Array::Float(quantile_utilities_arrayd(
                            candidates.mapv(|v| n64(v as f64)),
                            data.mapv(|v| n64(v as f64)),
                            lower.map(|v| v.array()?.first_float().map(n64)).transpose()?,
                            upper.map(|v| v.array()?.first_float().map(n64)).transpose()?,
                            self.alpha as Float)?)),
                    (Array::Int(candidates), Array::Int(data)) =>
                        Value::Array(Array::Float(quantile_utilities_arrayd(
                            candidates,
                            data,
                            lower.map(|v| v.array()?.first_int()).transpose()?,
                            upper.map(|v| v.array()?.first_int()).transpose()?,
                            self.alpha as Float)?)),
                    _ => return Err("data must be either f64 or i64".into())
                }
            },
            None => match data {
                Array::Float(data) =>
                    quantile(data.mapv(|v| n64(v as f64)), self.alpha, &self.interpolation)?
                        .mapv(|v| v.raw() as Float).into(),
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


pub fn quantile_utilities_arrayd<T: Ord + Clone + Copy>(
    candidates: ArrayD<T>, data: ArrayD<T>, lower: Option<T>, upper: Option<T>,
    alpha: Float
) -> Result<ArrayD<Float>> {

    Ok(ndarray::Array::from_shape_vec(candidates.shape(), candidates.gencolumns().into_iter()
        .zip(data.gencolumns().into_iter())
        .map(|(candidates, column)|
            quantile_utilities(candidates.to_vec(), column.to_vec(), lower, upper, alpha))
        .collect::<Result<Vec<Vec<_>>>>()?.into_iter()
        .flatten().collect::<Vec<_>>())?.into_dyn())
}

pub fn quantile_utilities<T: Ord + Clone + Copy>(
    mut candidates: Vec<T>, mut data: Vec<T>,
    lower: Option<T>, upper: Option<T>,
    alpha: Float
) -> Result<Vec<Float>> {
    match (lower, upper) {
        (Some(l), Some(u)) => {
            if l > u { return Err("lower must not be greater than upper".into()) }
            candidates.push(l);
            candidates.push(u);
            data.iter_mut().for_each(|v| *v = l.max(*v).min(u));
        }
        _ => ()
    }

    let n = data.len() as Float;
    let constant = alpha.max(1. - alpha);
    let mut candidates = candidates.into_iter().enumerate().collect::<Vec<(usize, T)>>();
    candidates.sort_unstable_by_key(|v| v.1);
    data.sort_unstable();

    let mut offsets = Vec::new();
    let mut index = 0;
    data.into_iter().enumerate().for_each(|(offset, v)| {
        while index < candidates.len() && v > candidates[index].1 {
            offsets.push(offset as Float);
            index += 1;
        }
    });

    // ensure offsets and candidates have the same length by appending offsets for candidates greater than the maximum value of the dataset
    offsets.extend((0..candidates.len() - offsets.len()).map(|_| n));

    let utilities = offsets.into_iter()
        .map(|offset| constant * n - (offset - alpha * n).abs())
        .collect::<Vec<Float>>();

    // order the utilities by the order of the candidates before they were sorted
    Ok(candidates.into_iter().map(|(idx, _)| utilities[idx]).collect::<Vec<_>>())
}

#[cfg(test)]
pub mod test_quantile {
    use crate::components::quantile::quantile_utilities_arrayd;
    use ndarray::arr1;
    use noisy_float::types::n64;

    #[test]
    fn utility() {
        let utilities = quantile_utilities_arrayd(
            arr1(&[-10., -5., 0., 2., 5., 7., 10., 12.]).into_dyn().mapv(n64),
            arr1(&[0., 10., 5., 7., 6., 4., 3., 8., 7., 6., 5., 5.]).into_dyn().mapv(n64),
            None, None,
            0.5
        ).unwrap();

        println!("utilities {:?}", utilities);
    }
}