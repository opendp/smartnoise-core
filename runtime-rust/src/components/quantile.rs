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
        let data = get_argument(&arguments, "data")?.get_arraynd()?;

        match (get_argument(&arguments, "by"), get_argument(&arguments, "categories")) {
            (Ok(by), Ok(categories)) => match (by, categories) {
                (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
//                    (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(quantile_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(quantile_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::F64(by), Vector2DJagged::F64(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(quantile_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(quantile_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::I64(by), Vector2DJagged::I64(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(quantile_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(quantile_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::Str(by), Vector2DJagged::Str(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(quantile_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(quantile_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
                    _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
                },
                _ => return Err("by must be ArrayND and categories must be Vector2DJagged".into())
            }
            // neither by nor categories can be retrieved
            (Err(_), Err(_)) => match data {
                ArrayND::F64(data) => Ok(Value::ArrayND(ArrayND::F64(quantile(&data, &self.quantile)?))),
//                ArrayND::I64(data) => Ok(Value::ArrayND(ArrayND::I64(quantile(&data)?))),
                _ => return Err("data must be either f64 or i64".into())
            }
            (Ok(_by), Err(_)) => Err("aggregation's 'by' must be categorically clamped".into()),
            _ => Err("both by and categories must be defined, or neither".into())
        }
    }
}


/// Accepts data and returns nth quantile
///
/// # Arguments
/// * `data` - Array of data for which you would like the median
///
/// # Return
/// median of your data
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
