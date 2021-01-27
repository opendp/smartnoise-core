use std::f64::consts::PI;
use std::ops::Div;

use smartnoise_validator::components::gaussian_mechanism::get_analytic_gaussian_sigma;
use smartnoise_validator::errors::*;
use smartnoise_validator::Float;

use crate::utilities;
use crate::utilities::noise;

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
/// * `value` - Statistic to be privatized.
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `sensitivity` - Upper bound on the L1 sensitivity of the function you want to privatize.
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// # Return
/// A single value drawn from the Laplace distribution with scale sensitivity/epsilon centered about 0.
///
/// # Examples
/// ```
/// use smartnoise_runtime::utilities::mechanisms::laplace_mechanism;
/// let n = laplace_mechanism(22.3, 0.1, 2.0, false);
/// ```
pub fn laplace_mechanism(
    value: f64, epsilon: f64, sensitivity: f64, enforce_constant_time: bool
) -> Result<f64> {

    if sensitivity < 0. {
        return Err(format!("sensitivity ({}) must be non-negative", sensitivity).into());
    }
    if epsilon <= 0. {
        return Err(format!("epsilon ({}) must be positive", epsilon).into())
    }
    let scale: f64 = sensitivity / epsilon;
    noise::sample_laplace(0., scale, enforce_constant_time).map(|n| value + n)
}

/// Computes privatized value according to the Snapping mechanism
///
/// Developed as a variant of the Laplace mechanism which does not suffer from floating-point side channel attacks.
/// For more information, see [Mironov (2012)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf)
///
/// # Arguments
/// * `value` - Non-private value of the statistic to be privatized.
/// * `epsilon` - Desired privacy guarantee.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `min` - Lower bound on function value being privatized.
/// * `max` - Upper bound on function value being privatized.
/// * `binding_probability` - Optional. Probability of binding on the final clamp
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// # Returns
/// Result of snapping mechanism
///
/// # Example
/// ```
/// use smartnoise_runtime::utilities::mechanisms::snapping_mechanism;
/// let value: f64 = 50.0;
/// let epsilon: f64 = 1.0;
/// let min: f64 = -50.;
/// let max: f64 = 150.0;
/// let sensitivity: f64 = 1.0/1000.0;
/// let precision: i64 = 118;
/// snapping_mechanism(value, epsilon, sensitivity, min, max, None, false).unwrap();
/// println!("snapped value: {}", value);
/// ```
pub fn snapping_mechanism(
    mut value: f64, epsilon: f64, sensitivity: f64,
    min: f64, max: f64, binding_probability: Option<f64>,
    enforce_constant_time: bool
) -> Result<f64> {
    if sensitivity < 0. {
        return Err(format!("sensitivity ({}) must be non-negative", sensitivity).into());
    }
    if epsilon <= 0. {
        return Err(format!("epsilon ({}) must be positive", epsilon).into())
    }
    if min > max {
        return Err("lower may not be greater than upper".into())
    }

    let mut b = (max - min) / 2.;
    let shift = min + b;

    // ~~ preprocess ~~
    // (A) shift mechanism input to be about zero
    value -= shift;
    // (B) clamp by b
    value = num::clamp(value, -b.abs(), b.abs());
    // (C) scale by sensitivity, to convert quantity to sensitivity-1
    value /= sensitivity;
    b /= sensitivity;

    // ~~ internals ~~
    let (mut value, epsilon) = noise::apply_snapping_noise(value, epsilon, b, enforce_constant_time)?;

    if let Some(binding_probability) = binding_probability {
        b += (1.0 - 2.0 * (1.0 - binding_probability).ln()) / epsilon;
    }

    // ~~ postprocess ~~
    // (C) return to original scale
    value *= sensitivity;
    b *= sensitivity;
    // (B) re-clamp by b
    value = num::clamp(value, -b.abs(), b.abs());
    // (A) shift mechanism output back to original location
    value += shift;

    Ok(value)
}

// const ADDITIVE_GAUSS_CONST: f64 = 8. / 9. + (2. / PI).ln();
const ADDITIVE_GAUSS_CONST: f64 = 0.4373061836;

/// Returns noise drawn according to the Gaussian mechanism.
///
/// If using the standard guassian,
/// Let c = sqrt(2*ln(1.25/delta)). Noise is drawn from a Gaussian distribution with scale
/// sensitivity*c/epsilon and centered about 0.
/// For more information, see the Gaussian mechanism in
/// C. Dwork, A. Roth The Algorithmic Foundations of Differential Privacy, Chapter 3.5.3 Laplace versus Gauss p.53. August 2014.
///
/// If using the analytic gaussian,
/// the noise scale is derived using [Balle (2018)](https://arxiv.org/pdf/1805.06530.pdf).
///
/// NOTE: this implementation of Gaussian draws in likely non-private due to floating-point attacks
/// See [Mironov (2012)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf)
/// for more information on a similar attack of the Laplace mechanism.
///
/// # Arguments
/// * `value` - Statistic to be privatized.
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `delta` - Additive privacy loss parameter.
/// * `sensitivity` - Upper bound on the L2 sensitivity of the function you want to privatize.
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// # Return
/// A draw from Gaussian distribution with scale defined as above.
///
/// # Examples
/// ```
/// use smartnoise_runtime::utilities::mechanisms::gaussian_mechanism;
/// let n = gaussian_mechanism(22.3, 0.1, 0.0001, 2.0, false, false);
/// ```
pub fn gaussian_mechanism(
    value: f64,
    epsilon: f64, delta: f64, sensitivity: f64,
    analytic: bool,
    enforce_constant_time: bool
) -> Result<f64> {
    if epsilon <= 0. || delta <= 0. || sensitivity <= 0. {
        return Err(format!("epsilon ({}), delta ({}) and sensitivity ({}) must all be positive", epsilon, delta, sensitivity).into());
    }

    let scale = if analytic {
        get_analytic_gaussian_sigma(epsilon, delta, sensitivity)
    } else {
        sensitivity * (ADDITIVE_GAUSS_CONST + 2. * (1. / delta).ln()).sqrt() / epsilon
    };
    // this uses mpfr noise if available
    Ok(value + noise::sample_gaussian(0., scale, enforce_constant_time)?)
}

/// Returns noise drawn according to the Geometric mechanism.
///
/// Uses the Geometric mechanism as originally proposed in
/// [Ghosh, Roughgarden, & Sundarajan (2012)](https://theory.stanford.edu/~tim/papers/priv.pdf).
/// We are calling this the `simple_geometric_mechanism` because there is some hope that we will later
/// add other versions, such as those developed in [Balcer & Vadhan (2019)](https://arxiv.org/pdf/1709.05396.pdf)
///
/// # Arguments
/// * `value` - Statistic to be privatized.
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
/// use smartnoise_runtime::utilities::mechanisms::simple_geometric_mechanism;
/// let n = simple_geometric_mechanism(4, 0.1, 1., 0, 10, true);
/// ```
pub fn simple_geometric_mechanism(
    value: i64,
    epsilon: f64, sensitivity: f64,
    min: i64, max: i64,
    enforce_constant_time: bool
) -> Result<i64> {
    if epsilon < 0. || sensitivity < 0. {
        return Err(format!("epsilon ({}) and sensitivity ({}) must be positive", epsilon, sensitivity).into());
    }
    let scale: f64 = sensitivity / epsilon;
    let noised = value + noise::sample_simple_geometric_mechanism(scale, min, max, enforce_constant_time)?;

    Ok(if noised < min {min} else if noised > max { max } else { noised })
}

/// Returns data element according to the Exponential mechanism.
///
/// # Arguments
///
/// * `epsilon` - Multiplicative privacy loss parameter.
/// * `sensitivity` - L1 sensitivity of utility function.
/// * `candidate_set` - Data from which user wants an element returned.
/// * `utility` - Utility function used within the exponential mechanism.
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// NOTE: This implementation is likely non-private because of the difference between theory on
///       the real numbers and floating-point numbers. See [Ilvento 2019](https://arxiv.org/abs/1912.04222) for
///       more information on the problem and a proposed fix.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use smartnoise_runtime::utilities::mechanisms::exponential_mechanism;
/// // create utility function
/// pub fn utility(x:&f64) -> f64 {
///     let util = *x as f64;
///     return util;
/// }
///
///
/// // create sample data
/// let xs: Vec<f64> = vec![1., 2., 3., 4., 5.];
/// let utilities: Vec<f64> = xs.iter().map(utility).collect();
/// let ans = exponential_mechanism(1.0, 1.0, &xs, utilities, false);
/// # ans.unwrap();
/// ```
#[cfg(feature = "use-mpfr")]
pub fn exponential_mechanism<T>(
    epsilon: f64,
    sensitivity: f64,
    candidate_set: &[T],
    utilities: Vec<f64>,
    enforce_constant_time: bool
) -> Result<T> where T: Clone, {
    macro_rules! to_rug {($v:expr) => {rug::Float::with_val(53, $v)}}

    // get vector of e^(scaled util), then use to find probabilities
    let scaling = to_rug!(epsilon).div(to_rug!(2. * sensitivity));

    // establish selection probabilities for each element
    let e_util_vec: Vec<rug::Float> = utilities.into_iter()
        .map(|util| (to_rug!(util) * &scaling).exp())
        .collect();
    let sum_e_util_vec = to_rug!(rug::Float::sum(e_util_vec.iter()));
    let probability_vec: Vec<Float> = e_util_vec.into_iter()
        .map(|x| (x / sum_e_util_vec.clone()).to_f64() as Float)
        .collect();

    // sample element relative to probability
    utilities::sample_from_set(candidate_set, &probability_vec, enforce_constant_time)
}

#[cfg(not(feature = "use-mpfr"))]
pub fn exponential_mechanism<T>(
    epsilon: f64,
    sensitivity: f64,
    candidate_set: &[T],
    utilities: Vec<f64>,
    enforce_constant_time: bool
) -> Result<T> where T: Clone, {

    // get vector of e^(util), and sample_from_set accepts weights
    let weight_vec: Vec<f64> = utilities.into_iter()
        .map(|x| (epsilon * x / (2. * sensitivity)).exp()).collect();

    // sample element relative to probability
    utilities::sample_from_set(candidate_set, &weight_vec, enforce_constant_time)
}