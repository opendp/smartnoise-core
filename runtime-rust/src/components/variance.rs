use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use crate::utilities::utilities::get_num_columns;
use whitenoise_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;

impl Evaluable for proto::Variance {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        Ok(variance(&get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?.clone())?.into())
    }
}

pub fn variance(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let means: Vec<f64> = mean(&data)?.iter().map(|v| v.clone()).collect();

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