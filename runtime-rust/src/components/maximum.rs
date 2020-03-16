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
        match get_argument(&arguments, "data")?.get_arraynd()? {
            ArrayND::F64(data) => Ok(maximum(&data)?.into()),
//                ArrayND::I64(data) => Ok(maximum(&data)?.into()),
            _ => return Err("data must be either f64 or i64".into())
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
