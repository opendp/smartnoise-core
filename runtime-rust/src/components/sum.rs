use smartnoise_validator::errors::*;

use crate::NodeArguments;
use smartnoise_validator::base::{Array, ReleaseNode};
use smartnoise_validator::utilities::{take_argument};
use crate::components::Evaluable;
use smartnoise_validator::proto;
use ndarray::{ArrayD};
use std::ops::Add;
use crate::utilities::get_num_columns;
use num::Zero;

impl Evaluable for proto::Sum {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        match take_argument(&mut arguments, "data")?.array()? {
            Array::Float(data) => Ok(sum(&data)?.into()),
            Array::Int(data) => Ok(sum(&data)?.into()),
            _ => return Err("data must be either f64 or i64".into())
        }.map(ReleaseNode::new)
    }
}

/// Calculates sum for each column of the data.
///
/// # Arguments
/// * `data` - Data for which you would like the sum of each column.
///
/// # Return
/// Sum of each column of the data.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use smartnoise_runtime::components::sum::sum;
/// let data = arr2(&[ [1.,10.], [2., 20.], [3., 30.] ]).into_dyn();
/// let sums = sum(&data).unwrap();
/// assert!(sums == arr2(&[[6., 60.]]).into_dyn());
/// ```
pub fn sum<T: Add<T, Output=T> + Zero + Copy>(data: &ArrayD<T>) -> Result<ArrayD<T>> {
    let data = data.clone();

    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.fold(T::zero(), |sum, i| sum + *i)).collect::<Vec<T>>();

    let array = match data.ndim() {
        1 => ndarray::Array::from_shape_vec(vec![], means),
        2 => ndarray::Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means),
        _ => return Err("invalid data shape for Sum".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Sum result into an array".into())
    }
}
