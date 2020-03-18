use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use crate::components::Evaluable;
use whitenoise_validator::base::{Value, ArrayND, get_argument};
use std::convert::TryFrom;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use crate::components::row_max::broadcast_map;

impl Evaluable for proto::RowMin {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(broadcast_map(
                        &x, &y, &|l: &f64, r: &f64| l.min(*r))?.into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(broadcast_map(
                        &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?.into()),
                _ => Err("Min: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Min: Both arguments must be arrays.".into())
        }
    }
}