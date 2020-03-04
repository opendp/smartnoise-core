use yarrow_validator::errors::*;
use yarrow_validator::ErrorKind::{PrivateError, PublicError};

use ndarray::prelude::*;

use crate::utilities::noise;
use crate::utilities::utilities;

/// Returns noise drawn according to the Laplace distribution
///
/// Noise is drawn with scale sensitivity/epsilon and centered about 0.
/// For more information, see the Laplace mechanism in 
/// C. Dwork, A. Roth The Algorithmic Foundations of Differential Privacy, Chapter 3.3 The Laplace Mechanism p.30-37. August 2014.
///
/// NOTE: this implementation of Laplace draws is likely non-private due to floating-point attacks
/// See Mironov, Ilya. "On significance of the least significant bits for differential privacy." 
/// Proceedings of the 2012 ACM conference on Computer and communications security. 2012.
/// http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf
/// for more information 
///
/// # Arguments
///
/// * `epsilon` - privacy loss parameter
/// * `sensitivity` - bound on the L1 sensitivity of the function the noise is to be added to the results of.
///
/// # Return
/// Array of a single value drawn from the Laplace distribution with scale sensitivity/epsilon centered about 0.
///
/// # Examples
/// ```
/// use yarrow_runtime::utilities::mechanisms::laplace_mechanism;
/// let n = laplace_mechanism(&0.1, &2.0)?;
/// ```
pub fn laplace_mechanism(epsilon: &f64, sensitivity: &f64) -> Result<f64> {
    let scale: f64 = sensitivity / epsilon;
    let noise: f64 = noise::sample_laplace(0., scale)?;

    Ok(noise)
}

/// Returns noise drawn according to the Gaussian distribution
///
/// Let c = sqrt(2*ln(1.25/delta)). Noise is drawn from a Gaussian distribution with scale 
/// sensitivity*c/epsilon and centered about 0.
///
/// For more information, see the Gaussian mechanism in 
/// C. Dwork, A. Roth The Algorithmic Foundations of Differential Privacy, Chapter 3.5.3 Laplace versus Gauss p.53. August 2014.
///
/// NOTE: this implementation of Gaussian draws in likely non-private due to floating-point attacks
/// See Mironov, Ilya. "On significance of the least significant bits for differential privacy." 
/// Proceedings of the 2012 ACM conference on Computer and communications security. 2012.
/// http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf
/// for more information 
///
/// # Arguments
///
/// * `epsilon` - multiplicative privacy loss parameter
/// * `delta` - additive privacy loss parameter
/// * `sensitivity` - bound on the L2 sensitivity of the function the noise is to be added to the results of.
///
/// # Return
/// Array of a single value drawn from the Gaussian distribution with scale defined as above.
///
/// # Examples
/// ```
/// use yarrow_runtime::utilities::mechanisms::gaussian_mechanism;
/// let n = gaussian_mechanism(&0.1, &0.0001, &2.0)?;
/// ```
pub fn gaussian_mechanism(epsilon: &f64, delta: &f64, sensitivity: &f64) -> Result<f64> {
    let scale: f64 = sensitivity * (2. * (1.25 / delta).ln()).sqrt() / epsilon;
    let noise: f64 = noise::sample_gaussian(&0., &scale)?;
    Ok(noise)
}

/// Returns noise drawn according to geometric distribution
///
/// # Arguments
///
/// * `epsilon` - multiplicative privacy loss parameter
/// * `sensitivity` - bound on the L2 sensitivity of the function the noise is to be added to the results of.
/// * `count_min` - 
/// * `count_max` -
/// * `enforce_constant_time` - Boolean flag that indicates whether or not to run the noise generation algorithm in constant time. 
///                             If true, will run count_max-count_min number of times.
/// # Return
/// Array of a single value drawn from the Gaussian distribution with scale defined as above.
///
/// # Examples
/// ```
/// use yarrow_runtime::utilities::mechanisms::simple_geometric_mechanism;
/// let n = simple_geometric_mechanism(&0.1, &1., &0, &10, &true);
/// ```
pub fn simple_geometric_mechanism(epsilon: &f64, sensitivity: &f64, count_min: &i64, count_max: &i64, enforce_constant_time: &bool) -> Result<i64> {
    let scale: f64 = sensitivity / epsilon;
    let noise: i64 = noise::sample_simple_geometric_mechanism(&scale, &count_min, &count_max, &enforce_constant_time)?;
    Ok(noise)
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
/// let ans: f64 = exponential_mechanism(&1.0, &1.0, xs, &utility)?;
/// println!("{}", ans);
/// ```
pub fn exponential_mechanism<T>(
                         epsilon: &f64,
                         sensitivity: &f64,
                         candidate_set: ArrayD<T>,
                         utility: &dyn Fn(&T) -> f64
                         ) -> Result<T> where T: Copy, {

    // get vector of e^(util), then use to find probabilities
    let e_util_vec: Vec<f64> = candidate_set.iter()
        .map(|x| std::f64::consts::E.powf(epsilon * utility(x) / (2.0 * sensitivity))).collect();
    let sum_e_util_vec: f64 = e_util_vec.iter().sum();
    let probability_vec: Vec<f64> = e_util_vec.iter().map(|x| x / sum_e_util_vec).collect();

    // sample element relative to probability
    let candidate_vec: Vec<T> = candidate_set.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let elem: T = utilities::sample_from_set(&candidate_vec, &probability_vec)?;

    Ok(elem)
}