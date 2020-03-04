use yarrow_validator::errors::*;

use crate::components::Evaluable;
use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND};
use yarrow_validator::proto;


impl Evaluable for proto::Add {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match (get_argument(&arguments, "left")?, get_argument(&arguments, "right")?) {
            (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
                (ArrayND::F64(x), ArrayND::F64(y)) =>
                    Ok(Value::ArrayND(ArrayND::F64(x + y))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(x + y))),
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
                    Ok(Value::ArrayND(ArrayND::F64(x - y))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(x - y))),
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
                    Ok(Value::ArrayND(ArrayND::F64(x / y))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(x / y))),
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
                    Ok(Value::ArrayND(ArrayND::F64(x * y))),
                (ArrayND::I64(x), ArrayND::I64(y)) =>
                    Ok(Value::ArrayND(ArrayND::I64(x * y))),
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

impl Evaluable for proto::Negate {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::ArrayND(data) => match data {
                ArrayND::F64(x) =>
                    Ok(Value::ArrayND(ArrayND::F64(-x))),
                ArrayND::I64(x) =>
                    Ok(Value::ArrayND(ArrayND::I64(-x))),
                _ => Err("Negate: Argument must be numeric.".into())
            },
            _ => Err("Negate: Argument must be an array.".into())
        }
    }
}