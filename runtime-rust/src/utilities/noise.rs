use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse};
use ieee754::Ieee754;
use num;
use rug;
use std::{cmp, f64::consts};

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

pub fn sample_uniform(min: f64, max: f64) -> f64 {
    let mut buf: [u8; 8] = [0; 8];
    rand_bytes(&mut buf).unwrap();
    (LittleEndian::read_u64(&buf) as f64) / (std::u64::MAX as f64) * (max - min) + min
}

pub fn sample_uniform_snapping() -> f64 {
    /// Returns random sample from Uniform(0,1)
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
    let binary_string = snapping::get_bytes(7);
    let mantissa = &binary_string[0..52];

    // convert mantissa to integer
    let mantissa_int = u64::from_str_radix(mantissa, 2).unwrap();

    // Generate exponent
    let geom: (i16) = snapping::get_geom_prob_one_half();
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
    let bit:i64 = snapping::get_bytes(1)[0..1].parse().unwrap();
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
    /// an unbiased source of coin flips (get_geom_prob_one_half).
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

    // repeatedly flip coin (up to 1024 times) and identify index (0-based) of first heads
    let first_heads_index: i16 = snapping::get_geom_prob_one_half() - 1;

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

pub fn sample_censored_geometric_dist(prob: &f64, max_trials: &f64, enforce_constant_time: &bool) -> f64 {
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
    let mut n_trials: f64 = 1.0;
    let mut geom_return: f64 = 0.0;

    // generate bits until we find a 1
    while n_trials <= *max_trials {
        bit = sample_bit(prob);
        if bit == 1 {
            if geom_return == 0.0 {
                geom_return = n_trials;
                if enforce_constant_time == &false {
                    return geom_return;
                }
            }
        } else {
            n_trials += 1.;
        }
    }

    // set geom_return to max if we never saw a bit equaling 1
    if geom_return == 0.0 {
        geom_return = *max_trials; // could also set this equal to n_trials - 1.
    }

    return geom_return;
}

pub fn sample_simple_geometric_mechanism(epsilon: &f64, sensitivity: &f64, func_min: &f64, func_max: &f64, enforce_constant_time: &bool) -> f64 {
    /// Sample noise according to geometric mechanism.
    /// This function uses coin flips to sample from the geometric distribution,
    /// rather than using the inverse probability transform. This is done
    /// to avoid finite precision attacks.
    ///
    /// For this algorithm, the number of steps it takes to sample from the geometric
    /// is bounded above by (func_max - func_min).
    ///
    /// # Arguments
    /// * `epsilon` - privacy parameter
    /// * `sensitivity` - sensitivity of function to which you want to add noise
    /// * `func_min` - minimum value of function to which you want to add noise
    /// * `func_max` - maximum value of function to which you want to add noise
    /// * `enforce_constant_time` - boolean for whether or not to require the geometric to run for the maximum number of trials
    ///
    /// # Return
    /// noise according to the geometric mechanism
    ///
    /// # Example
    /// ```
    /// let geom_noise: f64 = sample_simple_geometric_mechanism(&1., &1., &0., &100., &false);
    /// ```

    let alpha: f64 = consts::E.powf(-*epsilon / *sensitivity);
    let max_trials: f64 = func_max - func_min;

    // return 0 noise with probability (1-alpha) / (1+alpha), otherwise sample from geometric
    let unif: f64 = sample_uniform(0., 1.);
    if unif < (1. - &alpha) / (1. + &alpha) {
        return 0.0;
    } else {
        // get random sign
        let sign: f64 = 2.0 * (sample_bit(&0.5) as f64) - 1.0;
        // sample from censored geometric
        let geom: f64 = sample_censored_geometric_dist(&(1. - alpha), &max_trials, enforce_constant_time);
        return sign * geom;
    }
}