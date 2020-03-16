use yarrow_validator::errors::*;

use crate::components::Evaluable;
use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use yarrow_validator::proto;
use std::ops::Rem;
use crate::components::row_max::broadcast_map;


impl Evaluable for proto::Add {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok((x + y).into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok((x + y).into()),
                _ => Err("Add: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Add: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Subtract {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok((x - y).into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok((x - y).into()),
                _ => Err("Subtract: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Subtract: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Divide {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok((x / y).into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok((x / y).into()),
                _ => Err("Divide: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Divide: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Multiply {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok((x * y).into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok((x * y).into()),
                _ => Err("Multiply: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Multiply: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Power {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let power: f64 = get_argument(&arguments, "right")?.get_first_f64()?;
        let data = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;
        Ok(Value::ArrayND(ArrayND::F64(data.mapv(|x| x.powf(power)))))
    }
}


impl Evaluable for proto::Log {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let base: f64 = get_argument(&arguments, "right")?.get_first_f64()?;
        let data = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;
        Ok(Value::ArrayND(ArrayND::F64(data.mapv(|x| x.log(base)))))
    }
}

impl Evaluable for proto::Modulo {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.div_euclid(*r))?.into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| match l.checked_div_euclid(*r) {
                        // TODO SECURITY: impute ints
                        Some(v) => v, None => 0
                    })?.into()),
                _ => Err("Modulo: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Modulo: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Remainder {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.rem_euclid(*r))?.into()),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l.rem(*r))?.into()),
                _ => Err("Remainder: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Remainder: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::And {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::Bool(x), ArrayND::Bool(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l && *r)?))),
                _ => Err("And: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("And: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Or {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::Bool(x), ArrayND::Bool(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l || *r)?))),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }
    }
}


impl Evaluable for proto::Negate {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::ArrayND(data) => match data {
                ArrayND::Bool(data) =>
                    Ok(Value::ArrayND(ArrayND::Bool(data.mapv(|v| !v)))),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::Equal {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::Bool(x), ArrayND::Bool(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &bool, r: &bool| l == r)?))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &i64, r: &i64| l == r)?))),
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &f64, r: &f64| l == r)?))),
                (ArrayND::Str(x), ArrayND::Str(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &String, r: &String| l == r)?))),
                _ => Err("Equal: Argument types are mismatched.".into())
            },
            _ => Err("Equal: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::LessThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &i64, r: &i64| l < r)?))),
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &f64, r: &f64| l < r)?))),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }
    }
}

impl Evaluable for proto::GreaterThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &i64, r: &i64| l > r)?))),
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::Bool(broadcast_map(&x, &y, &|l: &f64, r: &f64| l > r)?))),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }
    }
}


impl Evaluable for proto::Negative {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::ArrayND(data) => match data {
                ArrayND::F64(x) => Ok(Value::ArrayND(ArrayND::F64(-x))),
                ArrayND::I64(x) => Ok(Value::ArrayND(ArrayND::I64(-x))),
                _ => Err("Negative: Argument must be numeric.".into())
            },
            _ => Err("Negative: Argument must be an array.".into())
        }
    }
}