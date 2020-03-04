use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use crate::components::Evaluable;
use yarrow_validator::base::{Value};
use std::convert::TryFrom;
use ndarray::ArrayD;
use yarrow_validator::proto;

impl Evaluable for proto::RowMin {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::broadcast_map(
                        &x, &y, &|l: &f64, r: &f64| l.min(*r))?))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::broadcast_map(
                        &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?))),
                _ => Err("Min: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Min: Both arguments must be arrays.".into())
        }
    }
}