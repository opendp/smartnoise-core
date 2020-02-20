use ndarray::prelude::*;

use crate::utilities::noise;
use crate::utilities::utilities;

pub fn laplace_mechanism(epsilon: &f64, sensitivity: &f64) -> ArrayD<f64> {
    let scale: f64 = sensitivity / epsilon;
    let noise: f64 = noise::sample_laplace(0., scale);

    return arr1(&[noise]).into_dyn();
}

pub fn gaussian_mechanism(epsilon: &f64, delta: &f64, sensitivity: &f64) -> ArrayD<f64> {
    let scale: f64 = sensitivity * (2. * (1.25 / delta).ln()).sqrt() / epsilon;
    let noise: f64 = noise::sample_gaussian(0., scale);
    return arr1(&[noise]).into_dyn();
}

pub fn simple_geometric_mechanism(epsilon: &f64, sensitivity: &f64, count_min: &i64, count_max: &i64, enforce_constant_time: &bool) -> ArrayD<i64> {
    let scale: f64 = sensitivity / epsilon;
    let noise: i64 = noise::sample_simple_geometric_mechanism(&scale, &count_min, &count_max, &enforce_constant_time);
    return arr1(&[noise]).into_dyn();
}

/// Returns data element according to the exponential mechanism
///
/// # Arguments
///
/// * `epsilon` - privacy loss parameter
/// * `sensitivity` - sensitivity of utility function
/// * `candidate_set` - data from which user wants an element returned
/// * `utility` - utility function used within the exponential mechanism
///
/// NOTE: This implementation is likely non-private because of the difference between theory on
///       the real numbers and floating-point numbers. See https://arxiv.org/abs/1912.04222 for
///       more information on the problem and a proposed fix.
///
/// TODO: Implement Christina's base-2 exponential mechanism?
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::mechanisms::exponential_mechanism;
/// // create utility function
/// pub fn utility(x:&f64) -> f64 {
///     let util = *x as f64;
///     return util;
/// }
///
/// // create sample data
/// let xs: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5.]).into_dyn();
/// let ans: f64 = exponential_mechanism(&1.0, &1.0, xs, &utility);
/// println!("{}", ans);
/// ```
pub fn exponential_mechanism<T>(
                         epsilon: &f64,
                         sensitivity: &f64,
                         candidate_set: ArrayD<T>,
                         utility: &dyn Fn(&T) -> f64
                         ) -> T where T: Copy, {

    // get vector of e^(util), then use to find probabilities
    let e_util_vec: Vec<f64> = candidate_set.iter().map(|x| std::f64::consts::E.powf(epsilon * utility(x) / (2.0 * sensitivity))).collect();
    let sum_e_util_vec: f64 = e_util_vec.iter().sum();
    let probability_vec: Vec<f64> = e_util_vec.iter().map(|x| x / sum_e_util_vec).collect();

    // sample element relative to probability
    let candidate_vec: Vec<T> = candidate_set.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let elem: T = utilities::sample_from_set(&candidate_vec, &probability_vec);

    return elem;
}