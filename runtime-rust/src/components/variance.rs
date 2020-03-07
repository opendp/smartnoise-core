use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;

impl Evaluable for proto::Variance {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

        match (arguments.get("by"), arguments.get("categories")) {
            (Some(by), Some(categories)) => match (by, categories) {
//                (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
//                    (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(variance_grouped(&data, &by, &categories)?))),
//                    (ArrayND::F64(by), Vector2DJagged::F64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(variance_grouped(&data, &by, &categories)?))),
//                    (ArrayND::I64(by), Vector2DJagged::I64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(variance_grouped(&data, by, categories)?))),
//                    (ArrayND::Str(by), Vector2DJagged::Str(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(variance_grouped(&data, by, categories)?))),
//                    _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
//                }
                _ => Err("by must be ArrayND and categories must be Vector2DJagged".into())
            }
            (None, None) => Ok(Value::ArrayND(ArrayND::F64(variance(&data)?))),
            (Some(_by), None) => Err("aggregation's 'by' must be categorically clamped".into()),
            _ => Err("both by and categories must be defined, or neither".into())
        }
    }
}

pub fn variance(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let means: Vec<f64> = mean(&data)?.into_dimensionality::<Ix1>().unwrap().to_vec();
    // iterate over the generalized columns
    let variances = data.gencolumns().into_iter().zip(means)
        .map(|(column, mean)| column.iter()
                .fold(0., |sum, v| sum + (v - mean).powi(2)) / column.len() as f64)
        .collect::<Vec<f64>>();

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], variances),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], variances),
        _ => return Err("invalid data shape for Variance".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Variance result into an array".into())
    }
}