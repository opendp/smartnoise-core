use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse, Distribution};
use ieee754::Ieee754;
use num;
use rug;
use std::{cmp, f64::consts};
use core::f64::NAN;
use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::utilities::utilities;

/// Sample from Laplace distribution centered at shift and scaled by scale
///
/// # Arguments
/// 
/// * `shift` - f64, the center of the Laplace distribution 
/// * `scale` - f64, the scaling parameter of the Laplace distribution
///
/// # Example
/// ```
/// use yarrow_runtime::utilities::noise::sample_laplace;
/// let n:f64 = sample_laplace(0.0, 2.0);
/// ```
pub fn sample_laplace(shift: f64, scale: f64) -> f64 {
    let probability: f64 = sample_uniform(0., 1.);
    Laplace::new(shift, scale).inverse(probability)
}

/// Sample from Gaussian distribution centered at shift and scaled by scale
///
/// # Arguments
/// 
/// * `shift` - f64, the center of the Laplace distribution 
/// * `scale` - f64, the scaling parameter of the Laplace distribution
///
/// Return
/// f64 Gaussian random variable centered at shift and scaled at scale
/// 
/// # Example
/// ```
/// use yarrow_runtime::utilities::noise::sample_gaussian;
/// let n:f64 = sample_gaussian(0.0, 2.0);
/// ```
pub fn sample_gaussian(shift: f64, scale: f64) -> f64 {
    let probability: f64 = sample_uniform(0., 1.);
    Gaussian::new(shift, scale).inverse(probability)
}

/// Sample from truncated Gaussian distribution
/// We use inverse transform sampling, but only between the CDF
/// probabilities associated with the stated min/max truncation values
///
/// # Arguments
///
/// * `shift` - f64, the center of the distribution
/// * `scale` - f64, the scaling parameter of the distribution
/// * `min` - f64, the minimum value of random variables pulled from the distribution.
/// * `max` - f64, the maximum value of random variables pulled from the distribution
/// 
/// # Return
/// f64 random gaussian random variable truncated to [min,max]
///
/// # Example
/// ```
/// use yarrow_runtime::utilities::noise::sample_gaussian_truncated;
/// let n:f64 = sample_gaussian_truncated(1.0, 1.0, 0.0, 2.0);
/// assert!(n >= 0.0);
/// assert!(n <= 2.0);
/// ```
pub fn sample_gaussian_truncated(shift: f64, scale: f64, min: f64, max: f64) -> f64 {
    assert!(min <= max);
    assert!(scale > 0.0);
    let unif_min: f64 = Gaussian::new(shift, scale).distribution(min);
    let unif_max: f64 = Gaussian::new(shift, scale).distribution(max);
    let unif: f64 = sample_uniform(unif_min, unif_max);
    return Gaussian::new(shift, scale).inverse(unif);
}

// pub fn sample_uniform(min: f64, max: f64) -> f64 {
//     let mut buf: [u8; 8] = [0; 8];
//     rand_bytes(&mut buf).unwrap();
//     (LittleEndian::read_u64(&buf) as f64) / (std::u64::MAX as f64) * (max - min) + min
// }

/// Sample from uniform integers between min and max (inclusive)
/// # Arguments 
/// 
/// * `min` - &i64, minimum value of distribution to sample from
/// * `max` - &i64, maximum value of distribution to sample from
///
/// # Return
/// i64 random uniform variable between min and max (inclusive)
///
/// # Example
/// ``` 
/// use yarrow_runtime::utilities::noise::sample_uniform_int;
/// let n:f64 = sample_uniform_int(0.0, 2.0);
/// assert!(n >= 0.0);
/// assert!(n <= 2.0);
/// ```
pub fn sample_uniform_int(min: &i64, max: &i64) -> i64 {
    assert!(min <= max);

    // define number of possible integers we could sample and the maximum
    // number of bits it would take to represent them
    let n_ints: i64 = max - min + 1;
    let n_bits: i64 = ( (n_ints as f64).log2() ).ceil() as i64;

    // uniformly sample integers from the set {0, 1, ..., n_ints-1}
    // by uniformly creating binary strings of length "n_bits"
    // and rejecting integers that are too large
    let mut valid_int: bool = false;
    let mut uniform_int: i64 = 0;
    while valid_int == false {
        uniform_int = 0;
        // generate random bits and increase integer by appropriate power of 2
        for i in 0..n_bits {
            let mut bit: i64 = sample_bit(&0.5);
            uniform_int += bit * 2_i64.pow(i as u32);
        }
        if uniform_int < n_ints {
            valid_int = true;
        }
    }

    // return successfully generated integer, scaled to be within
    // the correct range
    return uniform_int + min;
}

/// Returns random sample from Uniform[min,max)
/// All notes below refer to the version that samples from [0,1), before the final scaling takes place
///
/// This algorithm is taken from Mironov (2012) http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.366.5957&rep=rep1&type=pdf
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
pub fn sample_uniform(min: f64, max: f64) -> f64 {

    assert!(min <= max);

    // Generate mantissa
    let binary_string = utilities::get_bytes(7);
    let mantissa = &binary_string[0..52];

    // convert mantissa to integer
    let mantissa_int = u64::from_str_radix(mantissa, 2).unwrap();

    // Generate exponent
    let geom: (i16) = sample_floating_point_probability_exponent();
    let mut exponent: (u16) = (-geom + 1023) as u16;

    // Generate uniform random number from (0,1)
    let uniform_rand = f64::recompose_raw(false, exponent, mantissa_int);

    return uniform_rand * (max - min) + min;
}

pub fn sample_uniform_with_seed(min: f64, max: f64, seed: [u8; 32]) -> f64 {
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    return rng.gen::<f64>() * (max - min) + min;
}

/// Sample a single bit with arbitrary probability of "success", using only
/// an unbiased source of coin flips (sample_floating_point_probability_exponent).
/// The strategy for doing this with 2 flips in expectation is described
/// at https://amakelov.wordpress.com/2013/10/10/arbitrarily-biasing-a-coin-in-2-expected-tosses/
///
/// # Arguments
/// * `prob` - probability of success (bit == 1)
///
/// # Return
/// a bit that is 1 with probability "prob"
pub fn sample_bit(prob: &f64) -> i64 {

    // ensure that prob is a valid probability
    assert!(prob >= &0.0 || prob <= &1.0);

    // repeatedly flip coin (up to 1023 times) and identify index (0-based) of first heads
    let first_heads_index: i16 = sample_floating_point_probability_exponent() - 1;

    // decompose probability into mantissa (string of bits) and exponent integer to quickly identify the value in the first_heads_index
    let (sign, exponent, mantissa) = prob.decompose_raw();
    let mantissa_string = format!("1{:052b}", mantissa); // add implicit 1 to mantissa
    let mantissa_vec: Vec<i64> = mantissa_string.chars().map(|x| x.to_digit(2).unwrap() as i64).collect();
    let num_leading_zeros = cmp::max(1022_i16 - exponent as i16, 0); // number of leading zeros in binary representation of prob

    // return value at index of interest
    if first_heads_index < num_leading_zeros {
        return 0;
    } else {
        let index: usize = (num_leading_zeros + first_heads_index) as usize;
        return mantissa_vec[index];
    }
}

/// Sample from the censored geometric distribution with parameter "prob" and maximum
/// number of trials "max_trials".
///
/// # Arguments
/// * `prob` - parameter for the geometric distribution, the probability of success on any given trials
/// * `max_trials` - the maximum number of trials allowed
/// * `enforce_constant_time` - whether or not to enforce the algorithm to run in constant time; if true,
///                             it will always run for "max_trials" trials
///
/// # Return
/// result from censored geometric distribution
///
/// # Example
/// ```
/// use yarrow_runtime::utilities::noise::sample_geometric_censored;
/// let geom: i64 = sample_geometric_censored(&0.1, &20, &false);
/// ```
pub fn sample_geometric_censored(prob: &f64, max_trials: &i64, enforce_constant_time: &bool) -> i64 {

    // ensure that prob is a valid probability
    assert!(prob >= &0.0 || prob <= &1.0);

    let mut bit: i64 = 0;
    let mut n_trials: i64 = 1;
    let mut geom_return: i64 = 0;

    // generate bits until we find a 1
    while n_trials <= *max_trials {
        bit = sample_bit(prob);
        if bit == 1 {
            if geom_return == 0 {
                geom_return = n_trials;
                if enforce_constant_time == &false {
                    return geom_return;
                }
            }
        } else {
            n_trials += 1;
        }
    }

    // set geom_return to max if we never saw a bit equaling 1
    if geom_return == 0 {
        geom_return = *max_trials; // could also set this equal to n_trials - 1.
    }

    return geom_return;
}

/// Return sample from a censored Geometric distribution with parameter p=0.5
///
/// The algorithm generates 1023 bits uniformly at random and returns the
/// index of the first bit with value 1. If all 1023 bits are 0, then
/// the algorithm acts as if the last bit was a 1 and returns 1023.
///
/// This method was written specifically to generate an exponent
/// for the floating point representation of a uniform random number on [0,1),
/// ensuring that the numbers are distributed proportionally to
/// their unit of least precision.
pub fn sample_floating_point_probability_exponent() -> i16 {

    let mut geom: i16 = 1023;
    // read bytes in one at a time, need 128 to fully generate geometric
    for i in 0..128 {
        // read random bytes
        let binary_string = utilities::get_bytes(1);
        let binary_char_vec: Vec<char> = binary_string.chars().collect();

        // find first element that is '1' and mark its overall index
        let first_one_index = binary_char_vec.iter().position(|&x| x == '1');
        let first_one_overall_index: i16;
        if first_one_index.is_some() {
            let first_one_index_int = first_one_index.unwrap() as i16;
            first_one_overall_index = 8*i + first_one_index_int;
        } else {
            first_one_overall_index = geom;
        }
        geom = cmp::min(geom, first_one_overall_index+1);
    }
    return geom;
}

/// Sample noise according to geometric mechanism.
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
/// use yarrow_runtime::utilities::noise::sample_simple_geometric_mechanism;
/// let geom_noise: i64 = sample_simple_geometric_mechanism(&1., &0, &100, &false);
/// ```
pub fn sample_simple_geometric_mechanism(scale: &f64, min: &i64, max: &i64, enforce_constant_time: &bool) -> i64 {

    assert!(min <= max);

    let alpha: f64 = consts::E.powf(-*scale);
    let max_trials: i64 = max - min;

    // return 0 noise with probability (1-alpha) / (1+alpha), otherwise sample from geometric
    let unif: f64 = sample_uniform(0., 1.);
    if unif < (1. - &alpha) / (1. + &alpha) {
        return 0;
    } else {
        // get random sign
        let sign: i64 = 2 * sample_bit(&0.5) - 1;
        // sample from censored geometric
        let geom: i64 = sample_geometric_censored(&(1. - alpha), &max_trials, enforce_constant_time);
        return sign * geom;
    }
}