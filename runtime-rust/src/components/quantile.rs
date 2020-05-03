use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
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
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match get_argument(arguments, "data")?.array()? {
            Array::F64(data) =>
                quantile(data.mapv(n64), &self.alpha, &self.interpolation)?.mapv(|v| v.raw()).into(),
            Array::I64(data) =>
                quantile(data.clone(), &self.alpha, &self.interpolation)?.into(),
            _ => return Err("data must be either f64 or i64".into())
        }))
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
/// assert!(median == arr1(& [1.0, 2.0, 3.0] ).into_dyn().mapv(n64));
/// ```
pub fn quantile<T: FromPrimitive + Ord + Clone + Sub<Output=T> + Mul<Output=T> + Div<Output=T> + Add<Output=T> + Rem<Output=T> + ToPrimitive>(
    mut data: ArrayD<T>, alpha: &f64, interpolation: &String
) -> Result<ArrayD<T>> {
    if &0. > alpha || alpha > &1. {
        return Err("q must be within [0, 1]".into());
    }

    match match interpolation.as_str() {
        "lower" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Lower),
        "higher" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Higher),
        "midpoint" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Midpoint),
        "nearest" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Nearest),
        "linear" => data.quantile_axis_mut(Axis(0), n64(*alpha), &interpolate::Linear),
        _ => return Err("interpolation type unrecognized".into())
    }  {
        Ok(quantiles) => Ok(quantiles),
        Err(_) => Err("unable to compute quantiles".into())
    }
}
