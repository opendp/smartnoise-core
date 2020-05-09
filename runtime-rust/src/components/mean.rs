use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use crate::utilities::get_num_columns;
use whitenoise_validator::proto;

impl Evaluable for proto::Mean {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(mean(
            get_argument(arguments, "data")?.array()?.f64()?
        )?.into()))
    }
}

/// Calculates the arithmetic mean of each column in the provided data.
///
/// # Arguments
/// * `data` - Data for which you want the mean.
///
/// # Return
/// Arithmetic mean(s) of the data in question.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::mean::mean;
/// let data = arr2(&[ [1.,10.], [2., 20.], [3., 30.] ]).into_dyn();
/// let means = mean(&data).unwrap();
/// assert!(means == arr2(&[[2., 20.]]).into_dyn());
/// ```
pub fn mean(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.mean()).collect::<Option<Vec<f64>>>()
        .ok_or_else(|| Error::from("attempted mean of an empty column"))?;

    // ensure means are of correct dimension
    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], means),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means),
        _ => return Err("invalid data shape for Mean".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Mean result into an array".into())
    }
}


#[cfg(test)]
mod test_mean {
    use ndarray::{arr2};
    use crate::components::mean::mean;
    #[test]
    fn test_mean() {
        let data = arr2(&[ [1.,10.], [2., 20.], [3., 30.] ]).into_dyn();
        let means = mean(&data).unwrap();
        assert!(means == arr2(&[[2., 20.]]).into_dyn());
    }
}