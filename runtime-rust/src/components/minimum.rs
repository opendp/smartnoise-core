use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use whitenoise_validator::proto;
use ndarray::{ArrayD};

use crate::utilities::get_num_columns;


impl Evaluable for proto::Minimum {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match get_argument(arguments, "data")?.array()? {
            Array::F64(data) => minimum(&data)?.into(),
//                ArrayND::I64(data) => Ok(minimum(&data)?.into()),
            _ => return Err("data must be either f64 or i64".into())
        }))
    }
}

/// Finds minimum value in each column of data.
///
/// # Arguments
/// * `data` - Data for which you want the minimum of each column.
///
/// # Return
/// The minimum value in each column.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::minimum::minimum;
/// let data = arr2(&[ [1., 4., 5.], [10., 40., 50.] ]).into_dyn();
/// let mins = minimum(&data).unwrap();
/// assert!(mins == arr2(&[ [1., 4., 5.] ]).into_dyn());
/// ```
pub fn minimum(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let data = data.clone();

    // iterate over the generalized columns
    let mins = data.gencolumns().into_iter()
        .map(|column| column.iter().fold(std::f64::INFINITY, |a, &b| a.min(b))).collect::<Vec<f64>>();

    let array = match data.ndim() {
        1 => ndarray::Array::from_shape_vec(vec![], mins),
        2 => ndarray::Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], mins),
        _ => return Err("invalid data shape for Minimum".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Minimum result into an array".into())
    }
}
