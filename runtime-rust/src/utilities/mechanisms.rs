use whitenoise_validator::errors::*;

use rug::{float::Constant, Float, ops::Pow};

use crate::utilities::noise;
use crate::utilities;

/// Returns noise drawn according to the Laplace mechanism
///
/// Noise is drawn with scale sensitivity/epsilon and centered about 0.
/// For more information, see the Laplace mechanism in
/// C. Dwork, A. Roth The Algorithmic Foundations of Differential Privacy, Chapter 3.3 The Laplace Mechanism p.30-37. August 2014.
///
/// NOTE: this implementation of Laplace draws is likely non-private due to floating-point attacks
/// See [Mironov (2012)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf)
/// for more information
///
/// # Arguments
///
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `sensitivity` - Upper bound on the L1 sensitivity of the function you want to privatize.
///
/// # Return
/// Array of a single value drawn from the Laplace distribution with scale sensitivity/epsilon centered about 0.
///
/// # Examples
/// ```
/// use whitenoise_runtime::utilities::mechanisms::laplace_mechanism;
/// let n = laplace_mechanism(&0.1, &2.0);
/// ```
pub fn laplace_mechanism(epsilon: &f64, sensitivity: &f64) -> Result<f64> {
    if epsilon < &0. || sensitivity < &0. {
        return Err(format!("epsilon ({}) and sensitivity ({}) must be positive", epsilon, sensitivity).into());
    }
    let scale: f64 = sensitivity / epsilon;
    let noise: f64 = noise::sample_laplace(0., scale);

    Ok(noise)
}

/// Returns noise drawn according to the Gaussian mechanism.
///
/// Let c = sqrt(2*ln(1.25/delta)). Noise is drawn from a Gaussian distribution with scale
/// sensitivity*c/epsilon and centered about 0.
///
/// For more information, see the Gaussian mechanism in
/// C. Dwork, A. Roth The Algorithmic Foundations of Differential Privacy, Chapter 3.5.3 Laplace versus Gauss p.53. August 2014.
///
/// NOTE: this implementation of Gaussian draws in likely non-private due to floating-point attacks
/// See [Mironov (2012)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf)
/// for more information on a similar attack of the Laplace mechanism.
///
/// # Arguments
///
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `delta` - Additive privacy loss parameter.
/// * `sensitivity` - Upper bound on the L2 sensitivity of the function you want to privatize.
///
/// # Return
/// A draw from Gaussian distribution with scale defined as above.
///
/// # Examples
/// ```
/// use whitenoise_runtime::utilities::mechanisms::gaussian_mechanism;
/// let n = gaussian_mechanism(&0.1, &0.0001, &2.0);
/// ```
pub fn gaussian_mechanism(epsilon: &f64, delta: &f64, sensitivity: &f64) -> Result<f64> {
    if epsilon < &0. || delta < &0. || sensitivity < &0. {
        return Err(format!("epsilon ({}), delta ({}) and sensitivity ({}) must all be positive", epsilon, delta, sensitivity).into());
    }
    let scale: f64 = sensitivity * (2. * (1.25 / delta).ln()).sqrt() / epsilon;
    let noise: f64 = noise::sample_gaussian(&0., &scale);
    Ok(noise)
}

/// Returns noise drawn according to the Geometric mechanism.
///
/// Uses the Geometric mechanism as originally proposed in
/// [Ghosh, Roughgarden, & Sundarajan (2012)](https://theory.stanford.edu/~tim/papers/priv.pdf).
/// We are calling this the `simple_geometric_mechanism` because there is some hope that we will later
/// add other versions, such as those developed in [Balcer & Vadhan (2019)](https://arxiv.org/pdf/1709.05396.pdf)
///
/// # Arguments
///
/// * `epsilon` - Multiplicative privacy loss parameter
/// * `sensitivity` - L1 sensitivity of function you want to privatize. The Geometric is typically used for counting queries, where sensitivity = 1.
/// * `min` - The minimum return you think possible.
/// * `max` - The maximum return you think possible.
/// * `enforce_constant_time` - Whether or not to run the noise generation algorithm in constant time.
///                             If true, will run max-min number of times.
/// # Return
/// A draw according to the Geometric mechanism.
///
/// # Examples
/// ```
/// use whitenoise_runtime::utilities::mechanisms::simple_geometric_mechanism;
/// let n = simple_geometric_mechanism(&0.1, &1., &0, &10, &true);
/// ```
pub fn simple_geometric_mechanism(
    epsilon: &f64, sensitivity: &f64,
    min: &i64, max: &i64,
    enforce_constant_time: &bool
) -> Result<i64> {
    if epsilon < &0. || sensitivity < &0. {
        return Err(format!("epsilon ({}) and sensitivity ({}) must be positive", epsilon, sensitivity).into());
    }
    let scale: f64 = sensitivity / epsilon;
    let noise: i64 = noise::sample_simple_geometric_mechanism(&scale, &min, &max, &enforce_constant_time);
    Ok(noise)
}

/// Returns data element according to the Exponential mechanism.
///
/// # Arguments
///
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `sensitivity` - L1 sensitivity of utility function.
/// * `candidate_set` - Data from which user wants an element returned.
/// * `utility` - Utility function used within the exponential mechanism.
///
/// NOTE: This implementation is likely non-private because of the difference between theory on
///       the real numbers and floating-point numbers. See [Ilvento 2019](https://arxiv.org/abs/1912.04222) for
///       more information on the problem and a proposed fix.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::utilities::mechanisms::exponential_mechanism;
/// // create utility function
/// pub fn utility(x:&f64) -> f64 {
///     let util = *x as f64;
///     return util;
/// }
///
/// // create sample data
/// let xs: Vec<f64> = vec![1., 2., 3., 4., 5.];
/// let ans = exponential_mechanism(&1.0, &1.0, xs, &utility);
/// # ans.unwrap();
/// ```
pub fn exponential_mechanism<T>(
    epsilon: &f64,
    sensitivity: &f64,
    candidate_set: &Vec<T>,
    utilities: Vec<f64>,
) -> Result<T> where T: Clone, {

    // get vector of e^(util), then use to find probabilities
    let rug_e = Float::with_val(53, Constant::Euler);
    let rug_eps = Float::with_val(53, epsilon);
    let rug_sens = Float::with_val(53, sensitivity);
    let e_util_vec: Vec<rug::Float> = utilities.iter()
        .map(|x| rug_e.clone().pow(rug_eps.clone() * Float::with_val(53, x) / (2.0 * rug_sens.clone()))).collect();
    let sum_e_util_vec: rug::Float = Float::with_val(53, Float::sum(e_util_vec.iter()));
    let probability_vec: Vec<f64> = e_util_vec.iter().map(|x| (x / sum_e_util_vec.clone()).to_f64()).collect();

    // sample element relative to probability
    let elem: T = utilities::sample_from_set(candidate_set, &probability_vec)?;

    Ok(elem)
}