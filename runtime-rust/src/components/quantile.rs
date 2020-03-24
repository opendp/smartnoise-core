use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, get_argument, ArrayND};
use crate::components::Evaluable;
use whitenoise_validator::proto;
use ndarray::{ArrayD, Axis};

use ndarray_stats::QuantileExt;
use ndarray_stats::interpolate;
use noisy_float::types::n64;
use num::FromPrimitive;


impl Evaluable for proto::Quantile {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")?.get_arraynd()? {
            ArrayND::F64(data) =>
                Ok(quantile(data.mapv(n64), &self.quantile)?.mapv(|v| v.raw()).into()),
            ArrayND::I64(data) =>
                Ok(quantile(data.clone(), &self.quantile)?.into()),
            _ => return Err("data must be either f64 or i64".into())
        }
    }
}


/// Accepts data and returns desired quantile of each column in the data.
///
/// # Arguments
/// * `data` - Array of data for which you would like the quantile.
/// * `q` - Desired quantile.
///
/// # Return
/// Quantile of interest for each column of your data.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::quantile::quantile;
/// let data: ArrayD<f64> = arr2(&[ [0., 1., 2.], [2., 3., 4.] ]).into_dyn();
/// let median = quantile(&data, &0.5).unwrap();
/// assert!(median == arr1(& [1.0, 2.0, 3.0] ).into_dyn());
/// ```
pub fn quantile<T: FromPrimitive + Ord + Clone>(mut data: ArrayD<T>, q: &f64) -> Result<ArrayD<T>> {
    if &0. > q || q > &1. {
        return Err("q must be within [0, 1]".into());
    }

    match data.quantile_axis_mut(Axis(0), n64(*q), &interpolate::Lower) {
        Ok(quantiles) => Ok(quantiles),
        Err(_) => Err("unable to compute quantiles".into())
    }
}
