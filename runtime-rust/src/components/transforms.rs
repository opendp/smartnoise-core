use smartnoise_validator::errors::*;

use crate::components::Evaluable;
use crate::NodeArguments;
use smartnoise_validator::base::{Value, Array, ReleaseNode};
use smartnoise_validator::utilities::take_argument;
use smartnoise_validator::{proto, Integer, Float};
use crate::utilities::broadcast_map;


impl Evaluable for proto::Abs {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match take_argument(&mut arguments, "data")? {
            Value::Array(data) => match data {
                Array::Float(data) =>
                    Ok(data.mapv(|v| v.abs()).into()),
                Array::Int(data) =>
                    Ok(data.mapv(|v| v.abs()).into()),
                _ => Err("Abs: The atomic type must be numeric".into())
            },
            _ => Err("Abs: The argument type must be an array".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Add {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l + r)?.into()),
                (Array::Int(x), Array::Int(y)) =>
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
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(x, y, &|l: &bool, r: &bool| *l && *r)?.into()),
                _ => Err("And: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("And: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Divide {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l / r)?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l / r)?.into()),
                _ => Err("Divide: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Divide: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Equal {
    #[allow(clippy::float_cmp)]
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(x, y, &|l: &bool, r: &bool| l == r)?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| l == r)?.into()),
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l == r)?.into()),
                (Array::Str(x), Array::Str(y)) =>
                    Ok(broadcast_map(x, y, &|l: &String, r: &String| l == r)?.into()),
                _ => Err("Equal: Argument types are mismatched.".into())
            },
            _ => Err("Equal: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::GreaterThan {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| l > r)?.into()),
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l > r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::LessThan {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| l < r)?.into()),
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l < r)?.into()),
                _ => Err("LessThan: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("LessThan: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Log {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let base = take_argument(&mut arguments, "base")?.array()?.float()?;
        let data = take_argument(&mut arguments, "data")?.array()?.float()?;
        Ok(ReleaseNode::new(broadcast_map(base, data, &|base, x| x.log(*base))?.into()))
    }
}


impl Evaluable for proto::Modulo {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l.rem_euclid(*r))?.into()),
                (Array::Int(x), Array::Int(y)) => {
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| l.rem_euclid(*r))?.into())
                }
                _ => Err("Modulo: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Modulo: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Multiply {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l * r)?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l * r)?.into()),
                _ => Err("Multiply: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Multiply: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Negate {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match take_argument(&mut arguments, "data")? {
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
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match take_argument(&mut arguments, "data")? {
            Value::Array(data) => match data {
                Array::Float(x) => Ok((-x).into()),
                Array::Int(x) => Ok((-x).into()),
                _ => Err("Negative: Argument must be numeric.".into())
            },
            _ => Err("Negative: Argument must be an array.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Or {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Bool(x), Array::Bool(y)) =>
                    Ok(broadcast_map(x, y, &|l: &bool, r: &bool| *l || *r)?.into()),
                _ => Err("Or: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Or: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Power {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "data")?, take_argument(&mut arguments, "radical")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l.powf(*r))?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l.pow(*r as u32))?.into()),
                _ => Err("Power: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Power: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::RowMax {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l.max(*r))?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| *l.max(r))?.into()),
                _ => Err("RowMax: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("RowMax: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::RowMin {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Float, r: &Float| l.min(*r))?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l: &Integer, r: &Integer| *l.max(r))?.into()),
                _ => Err("RowMin: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("RowMin: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}

impl Evaluable for proto::Subtract {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match (take_argument(&mut arguments, "left")?, take_argument(&mut arguments, "right")?) {
            (Value::Array(left), Value::Array(right)) => match (left, right) {
                (Array::Float(x), Array::Float(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                (Array::Int(x), Array::Int(y)) =>
                    Ok(broadcast_map(x, y, &|l, r| l - r)?.into()),
                _ => Err("Subtract: Either the argument types are mismatched or non-numeric.".into())
            },
            _ => Err("Subtract: Both arguments must be arrays.".into())
        }.map(ReleaseNode::new)
    }
}
