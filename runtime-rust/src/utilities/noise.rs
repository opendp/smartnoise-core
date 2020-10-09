use std::{cmp, f64::consts, mem};

use ieee754::Ieee754;
use noisy_float::types::n64;
use probability::distribution::{Inverse, Laplace};
#[cfg(not(feature="use-mpfr"))]
use probability::prelude::Gaussian;
#[cfg(feature="use-mpfr")]
use rug::{Float, rand::{ThreadRandGen, ThreadRandState}};

use whitenoise_validator::components::snapping_mechanism::{compute_precision, get_smallest_greater_or_eq_power_of_two, redefine_epsilon};
use whitenoise_validator::errors::*;
use whitenoise_validator::Integer;

use crate::utilities;
use crate::utilities::get_closest_multiple_of_lambda;

// Give MPFR ability to draw randomness from OpenSSL
#[cfg(feature="use-mpfr")]
struct GeneratorOpenSSL;

#[cfg(feature="use-mpfr")]
impl ThreadRandGen for GeneratorOpenSSL {
    fn gen(&mut self) -> u32 {
        let mut buffer = [0u8; 4];
        // impossible not to panic here
        //    cannot ignore errors with .ok(), because the buffer will remain 0
        utilities::fill_bytes(&mut buffer).unwrap();
        u32::from_ne_bytes(buffer)
    }
}

/// Return sample from a censored Geometric distribution with parameter p=0.5 without calling to sample_bit_prob.
/// 
/// The algorithm generates 1023 bits uniformly at random and returns the
/// index of the first bit with value 1. If all 1023 bits are 0, then
/// the algorithm acts as if the last bit was a 1 and returns 1022.
/// 
/// This is a less general version of the sample_geometric_censored function, designed to be used
/// only inside of the sample_bit_prob function. The major difference is that this function does not 
/// call sample_bit_prob itself (whereas sample_geometric_censored does), so having this more specialized
/// version allows us to avoid an infinite dependence loop.
pub fn censored_specific_geom(enforce_constant_time: bool) -> Result<i16> {

    Ok(if enforce_constant_time {
        let mut buffer = vec!(0_u8; 128);
        utilities::fill_bytes(&mut buffer)?;

        cmp::min(buffer.into_iter().enumerate()
            // ignore samples that contain no events
            .filter(|(_, sample)| sample > &0)
            // compute the index of the smallest event in the batch
            .map(|(i, sample)| 8 * i + sample.leading_zeros() as usize)
            // retrieve the smallest index
            .min()
            // return 1022 if no events occurred (slight dp violation w.p. ~2^-52)
            .unwrap_or(1022) as i16, 1022)

    } else {
        // retrieve up to 128 bytes, each containing 8 trials
        for i in 0..128 {
            let mut buffer = vec!(0_u8; 1);
            utilities::fill_bytes(&mut buffer)?;

            if buffer[0] > 0 {
                return Ok(cmp::min(i * 8 + buffer[0].leading_zeros() as i16, 1022))
            }
        }
        1022
    })
}

/// Sample a single bit with arbitrary probability of success
///
/// Uses only an unbiased source of coin flips.
/// The strategy for doing this with 2 flips in expectation is described [here](https://amakelov.wordpress.com/2013/10/10/arbitrarily-biasing-a-coin-in-2-expected-tosses/).
///
/// # Arguments
/// * `prob`- The desired probability of success (bit = 1).
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// # Return
/// A bit that is 1 with probability "prob"
///
/// # Examples
///
/// ```
/// // returns a bit with Pr(bit = 1) = 0.7
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(0.7, false);
/// # n.unwrap();
/// ```
/// ```should_panic
/// // fails because 1.3 not a valid probability
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(1.3, false);
/// # n.unwrap();
/// ```
/// ```should_panic
/// // fails because -0.3 is not a valid probability
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(-0.3, false);
/// # n.unwrap();
/// ```
pub fn sample_bit_prob(prob: f64, enforce_constant_time: bool) -> Result<bool> {

    // ensure that prob is a valid probability
    if prob < 0.0 || prob > 1.0 {return Err("probability is not within [0, 1]".into())}

    // decompose probability into mantissa and exponent integers to quickly identify the value in the first_heads_index
    let (_sign, exponent, mantissa) = prob.decompose_raw();

    // repeatedly flip fair coin (up to 1023 times) and identify index (0-based) of first heads
    let first_heads_index = censored_specific_geom(enforce_constant_time)?;

    // if prob == 1., return after retrieving censored_specific_geom, to protect constant time
    if exponent == 1023 { return Ok(true) }

    // number of leading zeros in binary representation of prob
    //    cast is non-saturating because exponent only uses first 11 bits
    //    exponent is bounded within [0, 1022] by check for valid probability
    let num_leading_zeros = 1022_i16 - exponent as i16;

    // 0 is the most significant/leftmost implicit bit in the mantissa/fraction/significand
    // 52 is the least significant/rightmost
    Ok(match first_heads_index - num_leading_zeros {
        // index into the leading zeros of the binary representation
        i if i < 0 => false,
        // bit index 0 is implicitly set in ieee-754 when the exponent is nonzero
        i if i == 0 => exponent != 0,
        // all other digits out-of-bounds are not float-approximated/are-implicitly-zero
        i if i > 52 => false,
        // retrieve the bit at `i` slots shifted from the left
        i => mantissa & (1_u64 << (52 - i as usize)) != 0
    })
}

/// Sample from the binomial distribution.
///
/// # Arguments
/// * `n` - Number of trials
/// * `prob`- The desired probability of success (bit = 1).
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time
///
/// # Return
/// Number of successful trials
pub fn sample_binomial(n: i64, prob: f64, enforce_constant_time: bool) -> Result<i64> {
    (0..n).try_fold(0, |sum, _|
        sample_bit_prob(prob, enforce_constant_time)
            .map(|v| sum + if v {1} else {0}))
}

#[cfg(test)]
mod test_sample_bit_prob {
    use ieee754::Ieee754;
    use itertools::Itertools;

    use crate::utilities::noise::{sample_bit_prob, sample_uniform};

    fn check_bit_vs_string_equal(value: f64) {
        let (_sign, _exponent, mut mantissa) = value.decompose_raw();
        let mantissa_string = format!("1{:052b}", mantissa); // add implicit 1 to mantissa
        let mantissa_vec: Vec<i64> = mantissa_string.chars()
            .map(|x| x.to_digit(2).unwrap() as i64).collect();

        let to_str = |v| if v {"1"} else {"0"};

        let vec_bits = (0..mantissa_string.len())
            .map(|idx| mantissa_vec[idx] != 0)
            .map(to_str).join("");

        // set the implicit 1
        mantissa |= 1u64 << 52;

        let log_bits = (0..mantissa_string.len())
            .map(|idx| mantissa & (1u64 << (52 - idx)) != 0u64)
            .map(to_str).join("");

        // println!("vec_bits: {:?}", vec_bits);
        // println!("log_bits: {:?}", log_bits);

        assert_eq!(vec_bits, log_bits);
    }

    #[test]
    fn random_bit_vs_string() {
        for _ in 0..1000 {
            let prob = sample_uniform(0., 1., false).unwrap();
            check_bit_vs_string_equal(prob)
        }
    }

    #[test]
    fn sample_bit_prob_random() {
        let trials = 10_000;
        (0..=100)
            .map(|i| 0.01 * i as f64)
            .map(|prob| (prob, (0..trials)
                .fold(1, |sum, _|
                    sum + sample_bit_prob(prob, false).unwrap() as i32) as f64
                / trials as f64))
            .map(|(prob, actual)| (prob, actual - prob))
            .filter(|(_, bias)| bias.abs() > 0.01)
            .for_each(|(prob, bias)| println!("expected: {:?}, bias: {:?}", prob, bias));
    }

    #[test]
    fn sample_bit_prob_edge() {
        for _ in 0..10_000 {
            assert!(!sample_bit_prob(0., false).unwrap());
            assert!(sample_bit_prob(1., false).unwrap());
        }
    }

    #[test]
    fn edge_cases_bit_vs_string() {
        check_bit_vs_string_equal(0.);
        check_bit_vs_string_equal(1.);
        check_bit_vs_string_equal(f64::MAX);
        check_bit_vs_string_equal(f64::MIN)
    }
}

pub fn sample_bit() -> Result<bool> {
    let mut buffer = [0u8; 1];
    utilities::fill_bytes(&mut buffer)?;
    Ok(buffer[0] & 1 == 1)
}


#[cfg(test)]
mod test_sample_bit {
    use crate::utilities::noise::sample_bit;

    #[test]
    fn test_sample_bit() {
        (0..100).for_each(|_| {
            dbg!(sample_bit().unwrap());
        });
    }
}

/// Sample from uniform integers between min and max (inclusive).
///
/// # Arguments
///
/// * `min` - &i64, minimum value of distribution to sample from
/// * `max` - &i64, maximum value of distribution to sample from
///
/// # Return
/// Random uniform variable between min and max (inclusive).
///
/// # Example
///
/// ```
/// // returns a uniform draw from the set {0,1,2}
/// use whitenoise_runtime::utilities::noise::sample_uniform_int;
/// let n = sample_uniform_int(0, 2).unwrap();
/// assert!(n == 0 || n == 1 || n == 2);
/// ```
///
/// ```should_panic
/// // fails because min > max
/// use whitenoise_runtime::utilities::noise::sample_uniform_int;
/// let n = sample_uniform_int(2, 0);
/// # n.unwrap();
/// ```
pub fn sample_uniform_int(min: Integer, max: Integer) -> Result<Integer> {

    if min > max {return Err("min may not be greater than max".into());}

    // define number of possible integers we could sample and the maximum
    // number of bits it would take to represent them
    let n_ints: Integer = max - min + 1;
    let n_bytes = ((n_ints as f64).log2()).ceil() as usize / 8 + 1;

    // uniformly sample integers from the set {0, 1, ..., n_ints-1}
    // by filling the first n_bytes of a buffer with noise,
    // interpreting the buffer as an i64,
    // and rejecting integers that are too large
    let mut buffer = [0u8; mem::size_of::<Integer>()];
    loop {
        utilities::fill_bytes(&mut buffer[..n_bytes])?;
        let uniform_int = i64::from_le_bytes(buffer);
        if uniform_int < n_ints {
            return Ok(uniform_int + min)
        }
    }
}


#[cfg(test)]
mod test_sample_uniform_int {
    use crate::utilities::noise::sample_uniform_int;

    #[test]
    fn test_sample_bit() {
        (0..1_000).for_each(|_| {
            println!("{:?}", sample_uniform_int(0, 100).unwrap());
        });
    }
}


/// Returns random sample from Uniform[min,max).
///
/// All notes below refer to the version that samples from [0,1), before the final scaling takes place.
///
/// This algorithm is taken from [Mironov (2012)](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf)
/// and is important for making some of the guarantees in the paper.
///
/// The idea behind the uniform sampling is to first sample a "precision band".
/// Each band is a range of floating point numbers with the same level of arithmetic precision
/// and is situated between powers of two.
/// A band is sampled with probability relative to the unit of least precision using the Geometric distribution.
/// That is, the uniform sampler will generate the band [1/2,1) with probability 1/2, [1/4,1/2) with probability 1/4,
/// and so on.
///
/// Once the precision band has been selected, floating numbers numbers are generated uniformly within the band
/// by generating a 52-bit mantissa uniformly at random.
///
/// # Arguments
///
/// `min`: f64 minimum of uniform distribution (inclusive)
/// `max`: f64 maximum of uniform distribution (non-inclusive)
///
/// # Return
/// Random draw from Unif[min, max).
///
/// # Example
/// ```
/// // valid draw from Unif[0,2)
/// use whitenoise_runtime::utilities::noise::sample_uniform;
/// let unif = sample_uniform(0.0, 2.0, false);
/// # unif.unwrap();
/// ```
/// ``` should_panic
/// // fails because min > max
/// use whitenoise_runtime::utilities::noise::sample_uniform;
/// let unif = sample_uniform(2.0, 0.0, false);
/// # unif.unwrap();
/// ```
pub fn sample_uniform(min: f64, max: f64, enforce_constant_time: bool) -> Result<f64> {

    if min > max {return Err("min may not be greater than max".into());}

    // Generate mantissa
    let mut mantissa_buffer = [0u8; 8];
    // mantissa bit index zero is implicit
    utilities::fill_bytes(&mut mantissa_buffer[1..])?;
    // limit the buffer to 52 bits
    mantissa_buffer[1] %= 16;

    // convert mantissa to integer
    let mantissa_int = u64::from_be_bytes(mantissa_buffer);

    // Generate exponent. A saturated mantissa with implicit bit is ~2
    let exponent: i16 = -(1 + censored_specific_geom(enforce_constant_time)?);

    // Generate uniform random number from [0,1)
    let uniform_rand = f64::recompose(false, exponent, mantissa_int);
    Ok(uniform_rand * (max - min) + min)
}


#[cfg(test)]
mod test_uniform {
    use crate::utilities::noise::sample_uniform;

    #[test]
    fn test_uniform() {
        // (1..=100).for_each(|idx| println!("{:?}", (1. / 100. * idx as f64).decompose()));
        // println!("{:?}", 1.0f64.decompose());

        let min = 0.;
        let max = 1.;
        if !(0..1000).all(|_| {
            let sample = sample_uniform(min, max, false).unwrap();
            let within = min <= sample && max >= sample;
            if !within {
                println!("value outside of range: {:?}", sample);
            }
            within
        }) {
            panic!("not all numbers are within the range")
        }
    }

    #[test]
    fn test_endian() {

        use ieee754::Ieee754;
        let old_mantissa = 0.192f64.decompose().2;
        let mut buffer = old_mantissa.to_be_bytes();
        // from str_radix ignores these extra bits, but reconstruction from_be_bytes uses them
        buffer[1] = buffer[1] + 32;
        println!("{:?}", buffer);

        let new_buffer = buffer.iter()
            .map(|v| format!("{:08b}", v))
            .collect::<Vec<String>>();
        println!("{:?}", new_buffer);
        let new_mantissa = u64::from_str_radix(&new_buffer.concat(), 2).unwrap();
        println!("{:?} {:?}", old_mantissa, new_mantissa);

        let int_bytes = 12i64.to_le_bytes();
        println!("{:?}", int_bytes);
    }
}

/// Returns random sample from Uniform[min,max) using the MPFR library.
///
/// If [min, max) == [0, 1),then this is done in a way that respects exact rounding.
/// Otherwise, the return will be the result of a composition of two operations that
/// respect exact rounding (though the result will not necessarily).
///
/// # Arguments
/// * `min` - Lower bound of uniform distribution.
/// * `max` - Upper bound of uniform distribution.
///
/// # Return
/// Draw from Unif[min, max].
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_uniform_mpfr;
/// let unif = sample_uniform_mpfr(0.0, 1.0);
/// # unif.unwrap();
/// ```
#[cfg(feature = "use-mpfr")]
pub fn sample_uniform_mpfr(min: f64, max: f64) -> Result<rug::Float> {
    // initialize 64-bit floats within mpfr/rug
    let mpfr_min = Float::with_val(53, min);
    let mpfr_max = Float::with_val(53, max);
    let mpfr_diff = Float::with_val(53, &mpfr_max - &mpfr_min);

    // initialize randomness
    let mut rng = GeneratorOpenSSL {};
    let mut state = ThreadRandState::new_custom(&mut rng);

    // generate Unif[0,1] according to mpfr standard, then convert to correct scale
    let mut unif = Float::with_val(53, Float::random_cont(&mut state));
    unif = unif.mul_add(&mpfr_diff, &mpfr_min);

    // return uniform
    Ok(unif)
}

/// Sample from Laplace distribution centered at shift and scaled by scale.
/// 
/// # Arguments
///
/// * `shift` - The expectation of the Laplace distribution.
/// * `scale` - The scaling parameter of the Laplace distribution.
///
/// # Return
/// Draw from Laplace(shift, scale).
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_laplace;
/// let n = sample_laplace(0.0, 2.0, false);
/// # n.unwrap();
/// ```
pub fn sample_laplace(shift: f64, scale: f64, enforce_constant_time: bool) -> Result<f64> {
    let probability: f64 = sample_uniform(0., 1., enforce_constant_time)?;
    Ok(Laplace::new(shift, scale).inverse(probability))
}

/// Sample from Gaussian distribution centered at shift and scaled by scale.
///
/// # Arguments
///
/// * `shift` - The expectation of the Gaussian distribution.
/// * `scale` - The scaling parameter (standard deviation) of the Gaussian distribution.
///
/// # Return
/// A draw from Gaussian(shift, scale).
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_gaussian;
/// let n = sample_gaussian(0.0, 2.0, false);
/// # n.unwrap();
/// ```
#[cfg(not(feature = "use-mpfr"))]
pub fn sample_gaussian(shift: f64, scale: f64, enforce_constant_time: bool) -> Result<f64> {
    let probability: f64 = sample_uniform(0., 1., enforce_constant_time)?;
    Ok(Gaussian::new(shift, scale).inverse(probability))
}

/// Generates a draw from a Gaussian(loc, scale) distribution using the MPFR library.
///
/// If shift = 0 and scale = 1, sampling is done in a way that respects exact rounding.
/// Otherwise, the return will be the result of a composition of two operations that
/// respect exact rounding (though the result will not necessarily).
///
/// # Arguments
/// * `shift` - The expectation of the Gaussian distribution.
/// * `scale` - The scaling parameter (standard deviation) of the Gaussian distribution.
///
/// # Return
/// Draw from Gaussian(loc, scale)
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_gaussian;
/// let gaussian = sample_gaussian(0.0, 1.0, false);
/// ```
#[cfg(feature = "use-mpfr")]
pub fn sample_gaussian(shift: f64, scale: f64, _enforce_constant_time: bool) -> Result<f64> {
    // initialize 64-bit floats within mpfr/rug
    // NOTE: We square the scale here because we ask for the standard deviation as the function input, but
    //       the mpfr library wants the variance. We ask for std. dev. to be consistent with the rest of the library.
    let mpfr_shift = Float::with_val(53, shift);
    let mpfr_scale = Float::with_val(53, Float::with_val(53, scale).square());

    // initialize randomness
    let mut rng = GeneratorOpenSSL {};
    let mut state = ThreadRandState::new_custom(&mut rng);

    // generate Gaussian(0,1) according to mpfr standard, then convert to correct scale
    let gauss = Float::with_val(64, Float::random_normal(&mut state));
    Ok(gauss.mul_add(&mpfr_scale, &mpfr_shift).to_f64())
}

/// Sample from truncated Gaussian distribution.
///
/// This function uses a rejection sampling approach.
/// This means that values outside of the truncation bounds are ignored, rather
/// than pushed to the bounds (as they would be for a censored distribution).
///
/// # Arguments
///
/// * `shift` - The expectation of the untruncated Gaussian distribution.
/// * `scale` - The scaling parameter (standard deviation) of the untruncated Gaussian distribution.
/// * `min` - The minimum value you want to allow to be sampled.
/// * `max` - The maximum value you want to allow to be sampled.
///
/// # Return
/// A draw from a Gaussian(shift, scale) truncated to [min, max].
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_gaussian_truncated;
/// let n= sample_gaussian_truncated(0.0, 1.0, 0.0, 2.0, false);
/// # n.unwrap();
/// ```
pub fn sample_gaussian_truncated(
    min: f64, max: f64, shift: f64, scale: f64,
    enforce_constant_time: bool
) -> Result<f64> {
    if min > max {return Err("lower may not be greater than upper".into());}
    if scale <= 0.0 {return Err("scale must be greater than zero".into());}

    // return draw from distribution only if it is in correct range
    loop {
        let trunc_gauss = sample_gaussian(shift, scale, enforce_constant_time)?;
        if trunc_gauss >= min && trunc_gauss <= max {
            return Ok(trunc_gauss)
        }
    }
}

/// Sample from the censored geometric distribution with parameter "prob" and maximum
/// number of trials "max_trials".
///
/// # Arguments
/// * `prob` - Parameter for the geometric distribution, the probability of success on any given trials.
/// * `max_trials` - The maximum number of trials allowed.
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time; if true,
///                             it will always run for "max_trials" trials.
///
/// # Return
/// A draw from the censored geometric distribution.
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_geometric_censored;
/// let geom = sample_geometric_censored(0.1, 20, false);
/// # geom.unwrap();
/// ```
pub fn sample_geometric_censored(prob: f64, max_trials: i64, enforce_constant_time: bool) -> Result<i64> {

    // ensure that prob is a valid probability
    if prob < 0.0 || prob > 1.0 {return Err("probability is not within [0, 1]".into())}

    let mut bit: bool;
    let mut n_trials: i64 = 0;
    let mut geom_return: i64 = 0;

    // generate bits until we find a 1
    // if enforcing the runtime of the algorithm to be constant, the while loop
    // continues after the 1 is found and just stores the first location of a 1 bit.
    while n_trials < max_trials {
        bit = sample_bit_prob(prob, enforce_constant_time)?;
        n_trials += 1;

        // If we haven't seen a 1 yet, set the return to the current number of trials
        if bit && geom_return == 0 {
            geom_return = n_trials;
            if !enforce_constant_time {
                return Ok(geom_return);
            }
        }
    }

    // set geom_return to max if we never saw a bit equaling 1
    if geom_return == 0 {
        geom_return = max_trials; // could also set this equal to n_trials - 1.
    }

    Ok(geom_return)
}

/// Sample noise according to geometric mechanism
///
/// This function uses coin flips to sample from the geometric distribution,
/// rather than using the inverse probability transform. This is done
/// to avoid finite precision attacks.
///
/// For this algorithm, the number of steps it takes to sample from the geometric
/// is bounded above by (max - min).
///
/// # Arguments
/// * `scale` - scale parameter
/// * `min` - minimum value of function to which you want to add noise
/// * `max` - maximum value of function to which you want to add noise
/// * `enforce_constant_time` - boolean for whether or not to require the geometric to run for the maximum number of trials
///
/// # Return
/// noise according to the geometric mechanism
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::utilities::noise::sample_simple_geometric_mechanism;
/// let geom_noise = sample_simple_geometric_mechanism(1., 0, 100, false);
/// ```
pub fn sample_simple_geometric_mechanism(
    scale: f64, min: i64, max: i64, enforce_constant_time: bool
) -> Result<i64> {

    let alpha: f64 = consts::E.powf(-1. / scale);
    let max_trials: i64 = max - min;

    // return 0 noise with probability (1-alpha) / (1+alpha), otherwise sample from geometric
    let unif: f64 = sample_uniform(0., 1., enforce_constant_time)?;
    Ok(if unif < (1. - alpha) / (1. + alpha) {
        0
    } else {
        // get random sign
        let sign: i64 = 2 * sample_bit()? as i64 - 1;
        // sample from censored geometric
        let geom: i64 = sample_geometric_censored(1. - alpha, max_trials, enforce_constant_time)?;
        sign * geom
    })
}

/// Apply noise to value according to the Snapping mechanism.
/// Sensitivity is assumed to be 1 in L1 space.
///
/// # Arguments
/// * `value` - Non-private value of the statistic to be privatized.
/// * `epsilon` - Desired privacy guarantee.
/// * `b` - Upper bound on function value being privatized.
/// * `enforce_constant_time` - Whether or not to enforce the algorithm to run in constant time;
///
/// # Returns
/// Value of statistic with noise applied according to the Snapping mechanism.
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::apply_snapping_noise;
/// let value: f64 = 50.0;
/// let epsilon: f64 = 1.0;
/// let b: f64 = 100.0;
/// let value = apply_snapping_noise(value, epsilon, b, false);
/// println!("snapped value: {:?}", value.unwrap());
/// ```
#[cfg(feature = "use-mpfr")]
pub fn apply_snapping_noise(
    mut value: f64, mut epsilon: f64, b: f64,
    enforce_constant_time: bool
) -> Result<(f64, f64)> {
    // must be computed before redefining epsilon
    let precision = compute_precision(epsilon)?;

    // ensure that precision is supported by the OS
    if precision > rug::float::prec_max() {
        return Err("Operating system does not support sufficient precision to use the Snapping Mechanism".into());
    }
    macro_rules! to_rug {($v:expr) => {rug::Float::with_val(precision, $v)}};

    // effective epsilon is reduced due to snapping mechanism
    epsilon = redefine_epsilon(epsilon, b, precision);
    if epsilon == 0.0 {
        return Err("epsilon is zero due to floating-point round-off".into())
    }

    let sign = if sample_bit()? {-1.} else {1.};
    // 1.0 because sensitivity has been scaled to one
    let lambda = 1.0 / epsilon;
    // draw from {d: d in Doubles && d in (0, 1)} with probability based on unit of least precision
    let u_star_sample = to_rug!(sample_uniform(0., 1., enforce_constant_time)?);

    // add noise
    //    rug is mandatory for ln
    //    rug is optional for sign * lambda
    value += (to_rug!(sign * lambda) * u_star_sample.ln()).to_f64();

    // snap to lambda
    let m = get_smallest_greater_or_eq_power_of_two(lambda)?;
    value = get_closest_multiple_of_lambda(value, m)?;

    Ok((value, epsilon))
}

#[cfg(not(feature = "use-mpfr"))]
pub fn snapping_mechanism(
    mechanism_input: &f64, epsilon: &f64, b: &f64, sensitivity: &f64
) -> Result<f64> {
    Err(Error::from("Crate must be compiled with gmp-mpfr to use the snapping mechanism."))
}

/// Sample noise from the Gumbel Distribution
///
/// Based on C implementation from https://github.com/numpy/numpy/blob/d329a66dbb9710aefd03cce6a8b0f46da51490ca/numpy/random/src/distributions/distributions.c
///
/// # Arguments
/// * `loc` - location parameter
/// * `scale` - scale parameter
///
/// # Return
///  Noise according to the Gumbel Distribution
pub fn sample_gumbel(loc: f64, scale: f64) -> f64 {
    let rug_loc = Float::with_val(120, loc);
    let rug_scale = Float::with_val(120, scale);
    let u = Float::with_val(120, sample_uniform_mpfr(0.0, 1.0).unwrap());
    // Accept if u > 0, otherwise reject and call function again
    if u.gt(&Float::with_val(120, 0.0)) {
        let negative_log = -(u.ln());
        let log_term = negative_log.ln();
        (-rug_scale.mul_add(&log_term, &rug_loc)).to_f64()
    } else {
        sample_gumbel(loc, scale)
    }
}

/// Shuffle a vector
///
pub fn shuffle<T>(vector: Vec<T>, enforce_constant_time: bool) -> Result<Vec<T>> {
    let mut vector = vector
        .into_iter()
        .map(|v| Ok((v, n64(sample_uniform(0., 1., enforce_constant_time)?))))
        .collect::<Result<Vec<_>>>()?;
    vector.sort_unstable_by_key(|v| v.1);
    Ok(vector.into_iter().map(|(v, _)| v).collect())
}
