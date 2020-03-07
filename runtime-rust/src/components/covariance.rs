use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};

use yarrow_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;

impl Evaluable for proto::Covariance {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let left = get_argument(&arguments, "left")?.get_arraynd()?.get_f64()?;
        let right = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;

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
            (None, None) => Ok(Value::ArrayND(ArrayND::F64(cross_covariance(&left, &right)?))),
            (Some(_by), None) => Err("aggregation's 'by' must be categorically clamped".into()),
            _ => Err("both by and categories must be defined, or neither".into())
        }
    }
}

pub fn cross_covariance(left: &ArrayD<f64>, right: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let left_means: Vec<f64> = mean(&left)?.into_dimensionality::<Ix1>().unwrap().to_vec();
    let right_means: Vec<f64> = mean(&right)?.into_dimensionality::<Ix1>().unwrap().to_vec();

    let covariance_vec = left.gencolumns().into_iter()
        .zip(left_means.iter())
        .flat_map(|(column_left, mean_left)|
            right.gencolumns().into_iter()
                .zip(right_means.iter())
                .map(|(column_right, mean_right)| column_left.iter()
                    .zip(column_right)
                    .fold(0., |sum, (val_left, val_right)|
                        sum + ((val_left - mean_left) * (val_right - mean_right))) / column_left.len() as f64)
                .collect::<Vec<f64>>())
        .collect::<Vec<f64>>();

    match Array::from_shape_vec((left_means.len(), right_means.len()), covariance_vec) {
        Ok(array) => Ok(array.into_dyn()),
        Err(_) => Err("unable to form cross-covariance matrix".into())
    }
}