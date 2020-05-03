use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use whitenoise_validator::proto;
use ndarray::{ArrayD};

use crate::utilities::get_num_columns;


impl Evaluable for proto::Maximum {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match get_argument(arguments, "data")?.array()? {
            Array::F64(data) => maximum(&data)?.into(),
            _ => return Err("data must be either f64 or i64".into())
        }))
    }
}

/// Finds maximum value in each column of data.
///
/// # Arguments
/// * `data` - Data for which you want the maximum of each column.
///
/// # Return
/// The maximum value in each column.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::maximum::maximum;
/// let data = arr2(&[ [1., 4., 5.], [10., 40., 50.] ]).into_dyn();
/// let maxes = maximum(&data).unwrap();
/// assert!(maxes == arr2(&[ [10., 40., 50.] ]).into_dyn());
/// ```
pub fn maximum(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let data = data.clone();

    // iterate over the generalized columns
    let maxes = data.gencolumns().into_iter()
        .map(|column| column.iter().fold(std::f64::NEG_INFINITY, |a, &b| a.max(b))).collect::<Vec<f64>>();

    let array = match data.ndim() {
        1 => ndarray::Array::from_shape_vec(vec![], maxes),
        2 => ndarray::Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], maxes),
        _ => return Err("invalid data shape for Maximum".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Maximum result into an array".into())
    }
}
