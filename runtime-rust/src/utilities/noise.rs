use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse};
use ieee754::Ieee754;
use num;
use rug;

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

pub fn sampling_snapping_noise(mechanism_input: &f64, epsilon: &f64, B: &f64, sensitivity: &f64, precision: &f64) -> f64 {
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