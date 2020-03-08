use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, get_argument, ArrayND};
use crate::components::Evaluable;
use yarrow_validator::proto;
use ndarray::{ArrayD, Array};
use std::ops::Add;
use crate::utilities::utilities::get_num_columns;
use num::Zero;

impl Evaluable for proto::Maximum {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?;

        match (get_argument(&arguments, "by"), get_argument(&arguments, "categories")) {
            (Ok(by), Ok(categories)) => match (by, categories) {
                (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
//                    (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(maximum_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(maximum_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::F64(by), Vector2DJagged::F64(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(maximum_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(maximum_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::I64(by), Vector2DJagged::I64(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(maximum_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(maximum_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
//                    (ArrayND::Str(by), Vector2DJagged::Str(categories)) => match data {
//                        ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(maximum_by(&data, &by, &categories)?))),
//                        ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(maximum_by(&data, &by, &categories)?))),
//                        _ => return Err("data must be either f64 or i64".into())
//                    }
                    _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
                },
                _ => return Err("by must be ArrayND and categories must be Vector2DJagged".into())
            }
            // neither by nor categories can be retrieved
            (Err(_), Err(_)) => match data {
                ArrayND::F64(data) => Ok(Value::ArrayND(ArrayND::F64(maximum(&data)?))),
//                ArrayND::I64(data) => Ok(Value::ArrayND(ArrayND::I64(maximum(&data)?))),
                _ => return Err("data must be either f64 or i64".into())
            }
            (Ok(_by), Err(_)) => Err("aggregation's 'by' must be categorically clamped".into()),
            _ => Err("both by and categories must be defined, or neither".into())
        }
    }
}


pub fn maximum(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let data = data.clone();

    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.iter().fold(std::f64::INFINITY, |a, &b| a.min(b))).collect::<Vec<f64>>();

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], means),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means),
        _ => return Err("invalid data shape for Maximum".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Maximum result into an array".into())
    }
}
