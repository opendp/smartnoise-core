use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse, Distribution};
use ieee754::Ieee754;
use num;
use rug;
use std::{cmp, f64::consts};
use core::f64::NAN;

use crate::utilities::utilities;
use crate::utilities::snapping;

pub fn sample_laplace(shift: f64, scale: f64) -> f64 {
    let probability: f64 = sample_uniform(0., 1.);
    Laplace::new(shift, scale).inverse(probability)
//    shift - scale * (sample - 0.5).signum() * (1. - 2. * (sample - 0.5).abs()).ln()
}

pub fn sample_gaussian(shift: f64, scale: f64) -> f64 {
    let probability: f64 = sample_uniform(0., 1.);
    Gaussian::new(shift, scale).inverse(probability)
}

pub fn sample_gaussian_truncated(shift: f64, scale: f64, min: f64, max: f64) -> f64 {
    /// Sample from truncated Gaussian distribution
    /// We use inverse transform sampling, but only between the CDF
    /// probabilities associated with the stated min/max truncation values

    let unif_min: f64 = Gaussian::new(shift, scale).distribution(min);
    let unif_max: f64 = Gaussian::new(shift, scale).distribution(max);
    let unif: f64 = sample_uniform(unif_min, unif_max);
    return Gaussian::new(shift, scale).inverse(unif);
}

pub fn sample_uniform(min: f64, max: f64) -> f64 {
    let mut buf: [u8; 8] = [0; 8];
    rand_bytes(&mut buf).unwrap();
    (LittleEndian::read_u64(&buf) as f64) / (std::u64::MAX as f64) * (max - min) + min
}

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

pub fn sample_uniform_snapping() -> f64 {
    /// Returns random sample from Uniform[0,1)
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

    // Generate mantissa
    let binary_string = utilities::get_bytes(7);
    let mantissa = &binary_string[0..52];

    // convert mantissa to integer
    let mantissa_int = u64::from_str_radix(mantissa, 2).unwrap();

    // Generate exponent
    let geom: (i16) = sample_floating_point_probability_exponent();
    let exponent: (u16) = (-geom + 1023) as u16;

    // Generate uniform random number from (0,1)
    let uniform_rand = f64::recompose_raw(false, exponent, mantissa_int);

    return uniform_rand;
}

pub fn sample_snapping_noise(mechanism_input: &f64, epsilon: &f64, B: &f64, sensitivity: &f64, precision: &f64) -> f64 {
    /// Get noise according to the snapping mechanism
    ///
    /// # Arguments
    /// * `mechanism_input` - non-private statistic calculation
    /// * `epsilon` - desired privacy guarantee
    /// * `B` - snapping bound
    /// * `sensitivity` - sensitivity for function to which mechanism is being applied
    /// * `precision` - amount of arithmetic precision to which we have access
    ///
    /// # Returns
    /// noise according to snapping mechanism
    ///
    /// # Example
    /// ```
    /// let mechanism_input: f64 = 50.0;
    /// let epsilon: f64 = 1.0;
    /// let B: f64 = 100.0;
    /// let sensitivity: f64 = 1.0/1000.0;
    /// let precision: f64 = 64.0;
    /// let snapping_noise = sampling_snapping_noise(&mechanism_input, &epsilon, &B, &sensitivity, &precision);
    /// println!("snapping noise: {}", snapping_noise);
    /// ```

    // ensure that precision is sufficient for exact rounding of log, then check that it is supported by the OS
    let u32_precision = *precision as u32;
    let u32_precision = std::cmp::min(u32_precision, 118_u32);
    if u32_precision > rug::float::prec_max() {
        panic!("Operating system does not support sufficient precision to use the Snapping Mechanism");
    }

    // scale mechanism input by sensitivity
    let mechanism_input_scaled = mechanism_input / sensitivity;

    // get parameters
    let (B_scaled, epsilon_prime, Lambda_prime, Lambda_prime_scaled, m) = snapping::parameter_setup(&epsilon, &B, &sensitivity, &precision);

    // generate random sign and draw from Unif(0,1)
    let bit:i64 = utilities::get_bytes(1)[0..1].parse().unwrap();
    let sign = (2*bit-1) as f64;
    let u_star_sample = sample_uniform_snapping();

    // clamp to get inner result
    let sign_precise = rug::Float::with_val(u32_precision, sign);
    let scale_precise = rug::Float::with_val(u32_precision, 1.0/epsilon_prime);
    let log_unif_precise = rug::Float::with_val(u32_precision, u_star_sample.ln());
    let inner_result:f64 = num::clamp(mechanism_input_scaled, -B_scaled.abs(), B_scaled.abs()) +
                           (sign_precise * scale_precise * log_unif_precise).to_f64();

    // perform rounding and snapping
    let inner_result_rounded = snapping::get_closest_multiple_of_Lambda(&inner_result, &m);
    let private_estimate = num::clamp(sensitivity * inner_result_rounded, -B_scaled.abs(), B_scaled.abs());
    let snapping_mech_noise = private_estimate - mechanism_input;

    return snapping_mech_noise;
}

pub fn sample_bit(prob: &f64) -> i64 {
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

pub fn sample_geometric_censored(prob: &f64, max_trials: &i64, enforce_constant_time: &bool) -> i64 {
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
    /// let geom: f64 = sample_censored_geometric(&0.1, &20., &false);
    /// ```

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

pub fn sample_floating_point_probability_exponent() -> i16 {
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

    let mut geom: (i16) = 1023;
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
            first_one_overall_index = 1023;
        }
        geom = cmp::min(geom, first_one_overall_index+1);
    }
    return geom;
}

pub fn sample_simple_geometric_mechanism(epsilon: &f64, count_min: &i64, count_max: &i64, enforce_constant_time: &bool) -> i64 {
    /// Sample noise according to geometric mechanism.
    /// This function uses coin flips to sample from the geometric distribution,
    /// rather than using the inverse probability transform. This is done
    /// to avoid finite precision attacks.
    ///
    /// For this algorithm, the number of steps it takes to sample from the geometric
    /// is bounded above by (count_max - count_min).
    ///
    /// # Arguments
    /// * `epsilon` - privacy parameter
    /// * `count_min` - minimum value of function to which you want to add noise
    /// * `count_max` - maximum value of function to which you want to add noise
    /// * `enforce_constant_time` - boolean for whether or not to require the geometric to run for the maximum number of trials
    ///
    /// # Return
    /// noise according to the geometric mechanism
    ///
    /// # Example
    /// ```
    /// let geom_noise: f64 = sample_simple_geometric_mechanism(&1., &1., &0., &100., &false);
    /// ```

    let alpha: f64 = consts::E.powf(-*epsilon);
    let max_trials: i64 = count_max - count_min;

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