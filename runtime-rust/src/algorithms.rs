use ndarray::prelude::*;
use ndarray_stats::SummaryStatisticsExt;
use ndarray::Zip;

use crate::utilities::noise;

pub fn dp_mean_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64) -> f64 {

    let sensitivity: f64 = (maximum - minimum) / num_records;

    let mean: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum))
        .mean().unwrap();

    let noise: f64 = noise::sample_laplace(0., sensitivity / epsilon);

    mean + noise
}

pub fn dp_variance_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64) -> f64 {

    let sensitivity: f64 = (num_records - 1.0) * ((maximum - minimum) / num_records).powi(2);

    let variance: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum))
        .central_moment(2).unwrap();

    let noise: f64 = noise::sample_laplace(0., sensitivity / epsilon);

    variance + noise
}

pub fn dp_moment_raw_laplace(
    epsilon: f64, num_records: f64,
    data: ArrayD<f64>,
    minimum: f64, maximum: f64,
    order: u32) -> f64 {

    let sensitivity: f64 = (maximum - minimum).powi(order as i32) / num_records;

    let moment: f64 = data
        .mapv(|v| num::clamp(v, minimum, maximum).powi(order as i32))
        .mean().unwrap();

    let noise: f64 = noise::sample_laplace(0., sensitivity / epsilon);

    moment + noise
}

pub fn dp_covariance(
    epsilon: f64, num_records: f64,
    data_x: ArrayD<f64>, data_y: ArrayD<f64>,
    minimum_x: f64, minimum_y: f64,
    maximum_x: f64, maximum_y: f64) -> f64 {

    let sensitivity: f64 = 2. * (num_records - 1.)
        / num_records * (maximum_x - minimum_x) * (maximum_y - minimum_y);

    let data_x = data_x.mapv(|v| num::clamp(v, minimum_x, maximum_x)).into_dimensionality::<Ix1>().unwrap();
    let data_y = data_y.mapv(|v| num::clamp(v, minimum_y, maximum_y)).into_dimensionality::<Ix1>().unwrap();

    let mean_x = data_x.mean().unwrap();
    let mean_y = data_y.mean().unwrap();

    let mut products = Array1::<f64>::zeros(data_x.len());
    Zip::from(&mut products).and(&data_x).and(&data_y)
        .apply(|total, &x, &y| *total += (x - mean_x) * (y - mean_y));

    let covariance = products.mean().unwrap();
    let noise: f64 = noise::sample_laplace(0., sensitivity / epsilon);

    covariance + noise
}

pub fn dp_exponential<T>(
                         data: ArrayD<T>,
                         epsilon: f64,
                         utility: &dyn Fn(&T) -> f64,
                         sensitivity: f64
                         ) -> T where T: Copy, {
    /// Returns data element according to the exponential mechanism
    ///
    /// # Arguments
    ///
    /// * `data` - data from which user wants an element returned
    /// * `epsilon` - privacy loss parameter
    /// * `utility` - utility function used within the exponential mechanism
    /// * `sensitivity` - sensitivity of utility function
    ///
    /// NOTE: This implementation is likely non-private because of the difference between theory on
    ///       the real numbers and floating-point numbers. See https://arxiv.org/abs/1912.04222 for
    ///       more information on the problem and a proposed fix.
    ///
    /// TODO: Implement Christina's base-2 exponential mechanism?
    ///
    /// # Example
    /// ```
    /// // create utility function
    /// pub fn utility(x:&f64) -> f64 {
    ///     let util = *x as f64;
    ///     return util;
    /// }
    ///
    /// // create sample data
    /// let xs: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5.]).into_dyn();
    /// let ans:f64 = exponential_mechanism(xs, 1.0, &utility, 1.0);
    /// println!("{}", ans);
    /// ```

    // get vector of e^(util), then use to find probabilities
    let e_util_vec: Vec<f64> = data.iter().map(|x| std::f64::consts::E.powf(epsilon * utility(x) / (2.0 * sensitivity))).collect();
    let sum_e_util_vec:f64 = e_util_vec.iter().sum();
    let probability_vec: Vec<f64> = e_util_vec.iter().map(|x| x / sum_e_util_vec).collect();

    // generate cumulative probability distribution
    let cumulative_probability_vec = probability_vec.iter().scan(0.0, |sum, i| {*sum += i; Some(*sum)}).collect::<Vec<_>>();

    // generate uniform random number on [0,1)
    let unif:f64 = noise::sample_uniform_snapping();

    // sample an element relative to its probability
    let mut return_index = 0;
    for i in 0..cumulative_probability_vec.len() {
        if unif <= cumulative_probability_vec[i] {
            return_index = i;
            break
        }
    }
    return data[return_index]
}

//pub fn dp_histogram(
//    epsilon: f64, num_records: f64,
//    data_x: ArrayD<f64>
//) -> u64 {
//
//}
