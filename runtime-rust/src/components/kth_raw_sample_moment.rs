use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, get_argument, ArrayND};
use crate::components::Evaluable;
use whitenoise_validator::proto;
use ndarray::{ArrayD, Array};
use crate::utilities::utilities::get_num_columns;
use crate::components::mean::mean;

use std::convert::TryFrom;

impl Evaluable for proto::KthRawSampleMoment {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
        let k = get_argument(&arguments, "k")?.get_first_i64()?;
        Ok(kth_raw_sample_moment(data, &k)?.into())
    }
}


/// Accepts data and returns sample estimate of kth raw moment for each column.
///
/// # Arguments
/// * `data` - Data for which you would like the kth raw moments.
/// * `k` - Number representing the moment you want.
///
/// # Return
/// kth sample moment for each column.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::kth_raw_sample_moment::kth_raw_sample_moment;
/// let data: ArrayD<f64> = arr2(&[ [1., 1., 1.], [2., 4., 6.] ]).into_dyn();
/// let second_moments = kth_raw_sample_moment(&data, &2).unwrap();
/// assert!(second_moments == arr1(&[5., 17., 37.]).into_dyn());
/// ```
pub fn kth_raw_sample_moment(data: &ArrayD<f64>, k: &i64) -> Result<ArrayD<f64>> {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;
    let k = match i32::try_from(*k) {
        Ok(v) => v, Err(_) => return Err("k: invalid size".into())
    };
    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // for each pairing, iterate over the cells
        .for_each(|mut column| column.iter_mut()
            // mutate the cell via the operator
            .for_each(|v| *v = v.powi(k)));

    mean(&data)
}