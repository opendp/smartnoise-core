use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND};
use crate::components::Evaluable;
use crate::utilities;
use yarrow_validator::proto;


impl Evaluable for proto::Median {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
        Ok(Value::ArrayND(ArrayND::F64(median(&data))))
    }
}

/// Accepts data and returns median
///
/// # Arguments
/// * `data` - Array of data for which you would like the median
///
/// # Return
/// median of your data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::median;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let median: ArrayD<f64> = median(&data);
/// println!("{}", median);
/// assert_eq!(median, arr1(&[8.5]).into_dyn());
/// ```
pub fn median(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let data = data.clone();

    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| {
            
            column.iter().fold(std::f64::NEG_INFINITY, |a, &b| a.max(b))
        }).collect::<Vec<f64>>();

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], means),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means),
        _ => return Err("invalid data shape for Minimum".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Minimum result into an array".into())
    }

    // create vector version of data, get length, and sort it
    let mut data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let n = data_vec.len();
    data_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // if even number of elements, return mean of the two middlemost elements
    // if odd number of elements, return middlemost element
    if n % 2 == 0 {
        return arr1(&[(data_vec[n/2 - 1] + data_vec[n/2]) / 2.0]).into_dyn();
    } else {
        return arr1(&[data_vec[n/2]]).into_dyn();
    }
}