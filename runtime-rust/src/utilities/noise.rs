use whitenoise_validator::errors::*;
use probability::distribution::{Laplace, Inverse};
use ieee754::Ieee754;
use std::{cmp, f64::consts, mem};

use crate::utilities;

#[cfg(feature="use-mpfr")]
use rug::{Float, rand::{ThreadRandGen, ThreadRandState}};

use whitenoise_validator::Integer;

#[cfg(not(feature="use-mpfr"))]
use probability::prelude::Gaussian;

// Give MPFR ability to draw randomness from OpenSSL
#[cfg(feature="use-mpfr")]
struct GeneratorOpenSSL;

#[cfg(feature="use-mpfr")]
impl ThreadRandGen for GeneratorOpenSSL {
    fn gen(&mut self) -> u32 {
        let mut buffer = [0u8; 4];
        utilities::fill_bytes(&mut buffer);
        u32::from_ne_bytes(buffer)
    }
}

/// Return sample from a censored Geometric distribution with parameter p=0.5 without calling to sample_bit_prob.
/// 
/// The algorithm generates 1023 bits uniformly at random and returns the
/// index of the first bit with value 1. If all 1023 bits are 0, then
/// the algorithm acts as if the last bit was a 1 and returns 1023.
/// 
/// This is a less general version of the sample_geometric_censored function, designed to be used
/// only inside of the sample_bit_prob function. The major difference is that this function does not 
/// call sample_bit_prob itself (whereas sample_geometric_censored does), so having this more specialized
/// version allows us to avoid an infinite dependence loop. 
pub fn censored_specific_geom() -> Result<i16> {
    let mut geom: i16 = 1022;
    // read bytes in one at a time, need 128 to fully generate geometric
    for i in 0..128 {
        // read random bytes
        let binary_string = utilities::get_bytes(1);
        let first_one_idx: Option<usize> = binary_string.chars().position(|x| x == '1');

        // find first element that is '1' and mark its overall index
        let first_one_idx = if let Some(first_one_idx) = first_one_idx {
            8 * i + first_one_idx as i16
        } else {
            geom
        };
        geom = cmp::min(geom, first_one_idx);
    }
    Ok(geom)
}

/// Sample a single bit with arbitrary probability of success
///
/// Uses only an unbiased source of coin flips.
/// The strategy for doing this with 2 flips in expectation is described [here](https://amakelov.wordpress.com/2013/10/10/arbitrarily-biasing-a-coin-in-2-expected-tosses/).
///
/// # Arguments
/// * `prob`- The desired probability of success (bit = 1).
///
/// # Return
/// A bit that is 1 with probability "prob"
///
/// # Examples
///
/// ```
/// // returns a bit with Pr(bit = 1) = 0.7
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(0.7);
/// # n.unwrap();
/// ```
/// ```should_panic
/// // fails because 1.3 not a valid probability
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(1.3);
/// # n.unwrap();
/// ```
/// ```should_panic
/// // fails because -0.3 is not a valid probability
/// use whitenoise_runtime::utilities::noise::sample_bit_prob;
/// let n = sample_bit_prob(-0.3);
/// # n.unwrap();
/// ```
pub fn sample_bit_prob(prob: f64) -> Result<i64> {

    // ensure that prob is a valid probability
    assert!(prob >= 0.0 && prob <= 1.0);

    // repeatedly flip fair coin (up to 1023 times) and identify index (0-based) of first heads
    let first_heads_index: i16 = censored_specific_geom()? - 1;

    // decompose probability into mantissa (string of bits) and exponent integer to quickly identify the value in the first_heads_index
    let (_sign, exponent, mantissa) = prob.decompose_raw();
    let mantissa_string = format!("1{:052b}", mantissa); // add implicit 1 to mantissa
    let mantissa_vec: Vec<i64> = mantissa_string.chars().map(|x| x.to_digit(2).unwrap() as i64).collect();
    let num_leading_zeros = cmp::max(1022_i16 - exponent as i16, 0); // number of leading zeros in binary representation of prob

    // return value at index of interest
    if first_heads_index < num_leading_zeros {
        Ok(0)
    } else {
        let index: usize = (first_heads_index - num_leading_zeros) as usize;
        if index > mantissa_vec.len() {
            Ok(0)
        } else {
            Ok(mantissa_vec[index])
        }
    }
}

pub fn sample_bit() -> bool {
    let mut buffer = [0u8; 1];
    utilities::fill_bytes(&mut buffer);
    buffer[0] & 1 == 1
}


#[cfg(test)]
mod test_sample_bit {
    use crate::utilities::noise::sample_bit;

    #[test]
    fn test_sample_bit() {
        (0..100).for_each(|_| {
            dbg!(sample_bit());
        });
    }
}

/// Sample from uniform integers between min and max (inclusive).
///
/// # Arguments
///
/// * `min` - Minimum value of distribution from which we sample.
/// * `max` - Maximum value of distribution from which we sample.
///
/// # Return
/// Random uniform variable between min and max (inclusive).
///
/// # Example
/// ```
/// // returns a uniform draw from the set {0,1,2}
/// use whitenoise_runtime::utilities::noise::sample_uniform_int;
/// let n = sample_uniform_int(0, 2);
/// # n.unwrap();
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
    // by uniformly creating binary strings of length "n_bits"
    // and rejecting integers that are too large
    let mut buffer = [0u8; mem::size_of::<Integer>()];
    loop {
        utilities::fill_bytes(&mut buffer[..n_bytes]);
        let uniform_int = i64::from_le_bytes(buffer);
        if uniform_int < n_ints {
            return Ok(uniform_int + min)
        }
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
/// `min` - Inclusive minimum of uniform distribution.
/// `max` - Non-inclusive maximum of uniform distribution.
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
    if min > max {return Err("lower may not be greater than upper".into());}

    // Generate mantissa
    let mut mantissa_buffer = [0u8; 8];
    utilities::fill_bytes(&mut mantissa_buffer[1..]);
    // limit the buffer to 52 bits
    mantissa_buffer[1] %= 16;

    // convert mantissa to integer
    let mantissa_int = u64::from_be_bytes(mantissa_buffer);

    // Generate exponent
    let exponent: i16 = -sample_geometric_censored(0.5, 1023, enforce_constant_time)? as i16;

    // Generate uniform random number from [0,1)
    let uniform_rand = f64::recompose(false, exponent, mantissa_int);
    Ok(uniform_rand * (max - min) + min)
}


#[cfg(test)]
mod test_uniform {
    use crate::utilities::noise::sample_uniform;
    use ieee754::Ieee754;

    #[test]
    fn test_uniform() {
        (1..=100).for_each(|idx| println!("{:?}", (1. / 100. * idx as f64).decompose()));
        // println!("{:?}", 1.0f64.decompose());

        let min = 0.;
        let max = 1.;
        if !(0..10).all(|_| {
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

/// Generates a draw from Unif[min, max] using the MPFR library.
///
/// If [min, max] == [0, 1],then this is done in a way that respects exact rounding.
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

/// Generates a draw from a Gaussian distribution using the MPFR library.
///
/// If [min, max] == [0, 1],then this is done in a way that respects exact rounding.
/// Otherwise, the return will be the result of a composition of two operations that
/// respect exact rounding (though the result will not necessarily).
///
/// # Arguments
/// * `shift` - The expectation of the Gaussian distribution.
/// * `scale` - The scaling parameter (standard deviation) of the Gaussian distribution.
///
/// # Return
/// Draw from Gaussian(min, max)
///
/// # Example
/// ```
/// use whitenoise_runtime::utilities::noise::sample_gaussian_mpfr;
/// let gaussian = sample_gaussian_mpfr(0.0, 1.0);
/// ```
#[cfg(feature = "use-mpfr")]
pub fn sample_gaussian_mpfr(shift: f64, scale: f64) -> rug::Float {
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
    gauss.mul_add(&mpfr_scale, &mpfr_shift)
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
/// ```
pub fn sample_laplace(shift: f64, scale: f64, enforce_constant_time: bool) -> f64 {
    // nothing in sample_uniform can throw an error
    let probability: f64 = sample_uniform(0., 1., enforce_constant_time).unwrap();
    Laplace::new(shift, scale).inverse(probability)
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
/// ```
#[cfg(not(feature = "use-mpfr"))]
pub fn sample_gaussian(shift: f64, scale: f64, enforce_constant_time: bool) -> f64 {
    let probability: f64 = sample_uniform(0., 1., enforce_constant_time).unwrap();
    Gaussian::new(shift, scale).inverse(probability)
}

#[cfg(feature = "use-mpfr")]
pub fn sample_gaussian(shift: f64, scale: f64, _enforce_constant_time: bool) -> f64 {
    sample_gaussian_mpfr(shift, scale).to_f64()
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
        let trunc_gauss = sample_gaussian(shift, scale, enforce_constant_time);
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

    let mut bit: i64;
    let mut n_trials: i64 = 0;
    let mut geom_return: i64 = 0;

    // generate bits until we find a 1
    // if enforcing the runtime of the algorithm to be constant, the while loop
    // continues after the 1 is found and just stores the first location of a 1 bit.
    while n_trials < max_trials {
        bit = sample_bit_prob(prob)?;
        n_trials += 1;

        // If we haven't seen a 1 yet, set the return to the current number of trials
        if bit == 1 && geom_return == 0 {
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
pub fn sample_simple_geometric_mechanism(scale: f64, min: i64, max: i64, enforce_constant_time: bool) -> i64 {

    let alpha: f64 = consts::E.powf(-1. / scale);
    let max_trials: i64 = max - min;

    // return 0 noise with probability (1-alpha) / (1+alpha), otherwise sample from geometric
    let unif: f64 = sample_uniform(0., 1., enforce_constant_time).unwrap();
    if unif < (1. - alpha) / (1. + alpha) {
        0
    } else {
        // get random sign
        let sign: i64 = 2 * sample_bit() as i64 - 1;
        // sample from censored geometric. Unwrap is safe because (1. - alpha) is bounded by 1.
        let geom: i64 = sample_geometric_censored(1. - alpha, max_trials, enforce_constant_time).unwrap();
        sign * geom
    }
}