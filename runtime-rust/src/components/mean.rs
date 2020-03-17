use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;

impl Evaluable for proto::Mean {
    /// Calculates the arithmetic mean of each column in the provided data.
    ///
    /// # Arguments
    /// * `data` - Data for which you want the mean.
    ///
    /// # Return
    /// Arithmetic mean(s) of the data in question.
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        Ok(mean(&get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?.clone())?.into())
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
/// use yarrow_runtime::components::mean::mean;
/// let data = arr2(&[ [1.,10.], [2., 20.], [3., 30.] ]).into_dyn();
/// let means = mean(&data).unwrap();
/// assert!(means[[0, 0]] == 2. && means[[0, 1]] == 20.);
/// ```
pub fn mean(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.mean()).collect::<Option<Vec<f64>>>()
        .ok_or::<Error>("attempted mean of an empty column".into())?;

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
