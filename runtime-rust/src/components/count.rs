use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};
use whitenoise_validator::proto;
use crate::utilities::utilities::get_num_columns;


impl Evaluable for proto::Count {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        Ok(match get_argument(&arguments, "data")?.get_arraynd()? {
            ArrayND::Bool(data) => count(&data)?.into(),
            ArrayND::F64(data) => count(&data)?.into(),
            ArrayND::I64(data) => count(&data)?.into(),
            ArrayND::Str(data) => count(&data)?.into()
        })
    }
}

/// Gets number of rows of data.
///
/// # Arguments
/// * `data` - Data for which you want a count.
///
/// # Return
/// Number of rows in data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::count::count;
/// let data = arr2(&[ [false, false, true], [true, true, true] ]).into_dyn();
/// let n = count(&data).unwrap();
/// assert!(n == arr2(&[ [2, 2, 2] ]).into_dyn());
/// ```
pub fn count<T: Clone>(data: &ArrayD<T>) -> Result<ArrayD<i64>> {

    // iterate over the generalized columns. Of course, all columns will share the same length
    let counts = data.gencolumns().into_iter()
        .map(|column| column.len() as i64).collect::<Vec<i64>>();

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], counts),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], counts),
        _ => return Err("invalid data shape for Count".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Count result into an array".into())
    }
}
