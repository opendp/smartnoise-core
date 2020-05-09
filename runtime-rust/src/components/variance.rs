use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::ReleaseNode;
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use crate::utilities::get_num_columns;
use whitenoise_validator::proto;
use crate::components::mean::mean;

impl Evaluable for proto::Variance {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let delta_degrees_of_freedom = if self.finite_sample_correction { 1 } else { 0 } as usize;
        Ok(ReleaseNode::new(variance(
            &get_argument(arguments, "data")?.array()?.f64()?.clone(),
            &delta_degrees_of_freedom
        )?.into()))
    }
}

/// Calculate estimate of variance for each column in data.
///
/// # Arguments
/// * `data` - Data for which you would like the variance for each column.
/// * `delta_degrees_of_freedom` - 0 for population, 1 for finite sample correction
///
/// # Return
/// Variance for each column in the data.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::variance::variance;
/// let data = arr2(&[ [1.,10.], [2., 20.], [3., 30.] ]).into_dyn();
/// let variances = variance(&data, &1).unwrap();
/// assert!(variances == arr2(&[[1., 100.]]).into_dyn());
/// ```
pub fn variance(data: &ArrayD<f64>, delta_degrees_of_freedom: &usize) -> Result<ArrayD<f64>> {

    let means: Vec<f64> = mean(&data)?.iter().map(|v| v.clone()).collect();

    // iterate over the generalized columns
    let variances = data.gencolumns().into_iter().zip(means)
        .map(|(column, mean)| column.iter()
                .fold(0., |sum, v| sum + (v - mean).powi(2)) / (column.len() - delta_degrees_of_freedom.clone()) as f64)
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