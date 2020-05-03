use whitenoise_validator::errors::*;

use crate::components::Evaluable;
use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use whitenoise_validator::proto;
use num::{CheckedDiv};

use crate::utilities::broadcast_map;
use crate::utilities::noise::sample_uniform_int;


impl Evaluable for proto::Abs {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match get_argument(arguments, "data")? {
            Value::Array(data) => match data {
                Array::F64(data) =>
                    Ok(data.mapv(|v| v.abs()).into()),
                Array::I64(data) =>
                    Ok(data.mapv(|v| v.abs()).into()),
                _ => Err("Abs: The atomic type must be numeric".into())
            },
            _ => Err("Abs: The argument type must be an array".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Add {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l + r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l + r)?.into()),
                (Array::Str(x), Array::Str(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| format!("{}{}", l, r))?.into()),
                _ => Err("Add: Either the argument types are mismatched or boolean.".into())
            },
            _ => Err("Add: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::And {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l && *r)?.into()),
                _ => Err("And: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("And: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Divide {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l / r)?.into()),
                (Array::I64(x), Array::I64(y)) => {
                    let lower = get_argument(arguments, "lower")?.first_i64()?;
                    let upper = get_argument(arguments, "upper")?.first_i64()?;
                    if lower > upper {return Err("lower may not be greater than upper".into());}
                    Ok(broadcast_map(x, y, &|l, r|
                        l.checked_div(r).unwrap_or_else(|| sample_uniform_int(&lower, &upper).unwrap()))?.into())
                }
                _ => Err("Divide: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Divide: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Equal {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
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
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::GreaterThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l > r)?.into()),
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l > r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::LessThan {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l < r)?.into()),
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l < r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Log {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let base = get_argument(arguments, "base")?.array()?.f64()?;
        let data = get_argument(arguments, "data")?.array()?.f64()?;
        Ok(ReleaseNode::new(broadcast_map(base, data, &|base, x| x.log(*base))?.into()))
    }
}


impl Evaluable for proto::Modulo {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.rem_euclid(*r))?.into()),
                (Array::I64(x), Array::I64(y)) => {
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| l.rem_euclid(*r))?.into())
                },
                _ => Err("Modulo: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Modulo: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Multiply {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x,  &y, &|l, r| l * r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x,  &y, &|l, r| l * r)?.into()),
                _ => Err("Multiply: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Multiply: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Negate {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match get_argument(arguments, "data")? {
            Value::Array(data) => match data {
                Array::Bool(data) =>
                    Ok(data.mapv(|v| !v).into()),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Negative {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match get_argument(arguments, "data")? {
            Value::Array(data) => match data {
                Array::F64(x) => Ok((-x).into()),
                Array::I64(x) => Ok((-x).into()),
                _ => Err("Negative: Argument must be numeric.".into())
            },
            _ => Err("Negative: Argument must be an array.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Or {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &bool, r: &bool| *l || *r)?.into()),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Power {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "data")?, get_argument(arguments, "radical")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x,  y, &|l, r| l.powf(*r))?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x,  y, &|l, r| l.pow(*r as u32))?.into()),
                _ => Err("Power: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Power: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::RowMax {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.max(*r))?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?.into()),
                _ => Err("RowMax: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("RowMax: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::RowMin {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &f64, r: &f64| l.min(*r))?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(&x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?.into()),
                _ => Err("RowMin: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("RowMin: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Subtract {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        match (get_argument(arguments, "left")?, get_argument(arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::F64(x), Array::F64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                (Array::I64(x), Array::I64(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                _ => Err("Subtract: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Subtract: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}
