use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::Value;
use crate::components::Evaluable;
use yarrow_validator::proto;

impl Evaluable for proto::KthRawSampleMoment {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
        let k = get_argument(&arguments, "k")?.get_first_i64()?;
        Ok(Value::ArrayND(ArrayND::F64(kth_raw_sample_moment(&data, &k))))
    }
}


/// Accepts data and returns sample estimate of kth raw moment
///
/// # Arguments
/// * `data` - Array of data for which you would like the kth raw moment
/// * `k` - integer representing moment you want
///
/// # Return
/// kth sample moment
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::kth_raw_sample_moment;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let third_moment: ArrayD<f64> = kth_raw_sample_moment(&data, &3);
/// println!("{}", third_moment);
/// ```
pub fn kth_raw_sample_moment(data: &ArrayD<f64>, k: &i64) -> ArrayD<f64> {

    assert!(k >= &0);
    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_to_kth_power: Vec<f64> = data_vec.iter().map(|x| x.powf(*k as f64)).collect();
    return mean(&arr1(&data_to_kth_power).into_dyn());
}