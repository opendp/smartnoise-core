use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument, Vector2DJagged};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};

use whitenoise_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;

impl Evaluable for proto::Covariance {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        if arguments.contains_key("data") {
            let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

            return Ok(Value::Vector2DJagged(Vector2DJagged::F64(matrix_covariance(&data)?
                .iter().map(|v| Some(v.clone())).collect())));
        }
        if arguments.contains_key("left") && arguments.contains_key("right") {
            let left = get_argument(&arguments, "left")?.get_arraynd()?.get_f64()?;
            let right = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;

            let cross_cov = matrix_cross_covariance(&left, &right)?;

            return Ok(cross_cov.into());
        }
        Err("insufficient data supplied to Covariance".into())
    }
}

pub fn matrix_covariance(data: &ArrayD<f64>) -> Result<Vec<Vec<f64>>> {

    let means: Vec<f64> = mean(&data)?.iter().map(|v| v.clone()).collect();

    let mut covariances: Vec<Vec<f64>> = Vec::new();
    data.gencolumns().into_iter().enumerate()
        .for_each(|(left_i, left_col)| {
            let mut col_covariances: Vec<f64> = Vec::new();
            data.gencolumns().into_iter().enumerate()
                .filter(|(right_i, right_col)| &left_i <= right_i)
                .for_each(|(right_i, right_col)|
                    col_covariances.push(covariance(&left_col, &right_col, &means[left_i].clone(), &means[right_i].clone())));
            covariances.push(col_covariances);
        });
    Ok(covariances)
}

pub fn matrix_cross_covariance(left: &ArrayD<f64>, right: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let left_means: Vec<f64> = mean(&left)?.iter().map(|v| v.clone()).collect();
    let right_means: Vec<f64> = mean(&right)?.iter().map(|v| v.clone()).collect();

    let covariances = left.gencolumns().into_iter()
        .zip(left_means.iter())
        .flat_map(|(column_left, mean_left)|
            right.gencolumns().into_iter()
                .zip(right_means.iter())
                .map(|(column_right, mean_right)| covariance(&column_left, &column_right, &mean_left, &mean_right))
                .collect::<Vec<f64>>())
        .collect::<Vec<f64>>();

    match Array::from_shape_vec((left_means.len(), right_means.len()), covariances) {
        Ok(array) => Ok(array.into_dyn()),
        Err(_) => Err("unable to form cross-covariance matrix".into())
    }
}

fn covariance(left: &ArrayView1<f64>, right: &ArrayView1<f64>, mean_left: &f64, mean_right: &f64) -> f64 {
    left.iter()
        .zip(right)
        .fold(0., |sum, (val_left, val_right)|
            sum + ((val_left - mean_left) * (val_right - mean_right))) / left.len() as f64
}