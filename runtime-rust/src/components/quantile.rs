use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, get_argument, ArrayND};
use crate::components::Evaluable;
use yarrow_validator::proto;
use ndarray::{ArrayD, Array, Axis};
use std::ops::Add;
use crate::utilities::utilities::get_num_columns;
use num::Zero;
use ndarray_stats::QuantileExt;
use ndarray_stats::interpolate;
use noisy_float::types::n64;
use ndarray_stats::interpolate::Interpolate;
use noisy_float::types::N64;

impl Evaluable for proto::Quantile {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")?.get_arraynd()? {
            ArrayND::F64(data) => Ok(quantile(&data, &self.quantile)?.into()),
//                ArrayND::I64(data) => Ok(quantile(&data)?.into()),
            _ => return Err("data must be either f64 or i64".into())
        }
    }
}


/// Accepts data and returns nth quantile
///
/// # Arguments
/// * `data` - Array of data for which you would like the quantile
///
/// # Return
/// quantile of your data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use crate::components::quantile;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let median: ArrayD<f64> = quantile(&data, &0.5);
/// println!("{}", median);
/// assert_eq!(median, arr1(&[8.5]).into_dyn());
/// ```
pub fn quantile(data: &ArrayD<f64>, q: &f64) -> Result<ArrayD<f64>> {
    if &0. > q || q > &1. {
        return Err("q must be within [0, 1]".into());
    }
    let mut data = data.mapv(n64);

    match data.quantile_axis_mut(Axis(0), n64(*q), &interpolate::Midpoint) {
        Ok(quantiles) => Ok(quantiles.mapv(|v| v.into())),
        Err(_) => Err("unable to compute quantiles".into())
    }
}
