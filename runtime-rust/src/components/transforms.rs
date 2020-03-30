use whitenoise_validator::errors::*;

use crate::components::Evaluable;
use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, Array};
use whitenoise_validator::utilities::get_argument;
use whitenoise_validator::proto;
use num::{CheckedDiv};

use crate::utilities::broadcast_map;
use crate::utilities::noise::sample_uniform_int;


impl Evaluable for proto::Add {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l + r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l + r)?.into()),
                _ => Err("Add: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Add: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Subtract {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                _ => Err("Subtract: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Subtract: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Divide {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l / r)?.into()),
                (Array::I64(x), Array::I64(y)) => {
                    let min = get_argument(arguments, "min")?.first_i64()?;
                    let max = get_argument(arguments, "max")?.first_i64()?;
                    if min > max {return Err("min may not be greater than max".into());}
                    Ok(broadcast_map(x, y, &|l, r|
                        l.checked_div(r).unwrap_or_else(|| sample_uniform_int(&min, &max).unwrap()))?.into())
                }
                _ => Err("Divide: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Divide: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Multiply {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x,  &y, &|l, r| l * r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x,  &y, &|l, r| l * r)?.into()),
                _ => Err("Multiply: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Multiply: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Power {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x,  y, &|l, r| l.powf(*r))?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x,  y, &|l, r| l.pow(*r as u32))?.into()),
                _ => Err("Power: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Power: Both arguments must be arrays.".into())
        }
    }
}


impl Evaluable for proto::Log {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let base = get_argument(&arguments, "right")?.array()?.f64()?;
        let data = get_argument(&arguments, "right")?.array()?.f64()?;
        Ok(broadcast_map(base, data, &|base, x| x.log(*base))?.into())
    }
}

impl Evaluable for proto::Modulo {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.rem_euclid(*r))?.into()),
                (Array::I64(x), Array::I64(y)) => {
                    let min = get_argument(arguments, "min")
                        .chain_err(|| "min must be known in case of imputation")?.first_i64()?;
                    let max = get_argument(arguments, "max")
                        .chain_err(|| "max must be known in case of imputation")?.first_i64()?;

                    if min > max {return Err("Modulo: min may not be greater than max".into());}
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64|
                        l.checked_rem_euclid(*r).unwrap_or_else(|| sample_uniform_int(&min, &max).unwrap()))?.into())
                },
                _ => Err("Modulo: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Modulo: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::And {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l && *r)?.into()),
                _ => Err("And: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("And: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Or {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l || *r)?.into()),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }
    }
}


impl Evaluable for proto::Negate {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::Array(data) => match data {
                Array::Bool(data) =>
                    Ok(data.mapv(|v| !v).into()),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Equal {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &bool, r: &bool| l == r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l == r)?.into()),
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l == r)?.into()),
                (Array::Str(x), Array::Str(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &String, r: &String| l == r)?.into()),
                _ => Err("Equal: Argument types are mismatched.".into())
            },
            _ => Err("Equal: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::LessThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l < r)?.into()),
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l < r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::GreaterThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l > r)?.into()),
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l > r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }
    }
}


impl Evaluable for proto::Negative {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::Array(data) => match data {
                Array::F64(x) => Ok((-x).into()),
                Array::I64(x) => Ok((-x).into()),
                _ => Err("Negative: Argument must be numeric.".into())
            },
            _ => Err("Negative: Argument must be an array.".into())
        }
    }
}