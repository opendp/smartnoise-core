use yarrow_validator::errors::*;

use openssl::rand::rand_bytes;
use ieee754::Ieee754;

use crate::utilities::noise;
use ndarray::{ArrayD};

use rug::Float;


/// Return bytes of binary data as String
///
/// Reads bytes from OpenSSL, converts them into a string,
/// concatenates them, and returns the combined string
///
/// # Arguments
/// * `n_bytes` - A numeric variable
///
/// # Return
/// * `binary_string` - String of n_bytes bytes
pub fn get_bytes(n_bytes: usize) -> String {
    // read random bytes from OpenSSL
    let mut buffer = vec!(0_u8; n_bytes);
    rand_bytes(&mut buffer).unwrap();

    // create new buffer of binary representations, rather than u8
    let mut new_buffer = Vec::new();
    for i in 0..buffer.len() {
        new_buffer.push(format!("{:08b}", buffer[i]));
    }

    // combine binary representations into single string and subset mantissa
    let binary_string = new_buffer.join("");

    return binary_string;
}

/// Converts f64 to String of length 64, yielding the IEEE-754 binary representation of the number
///
/// # Arguments
/// * `num` - a number of type f64
///
/// # Return
/// * `binary_string`: String showing IEEE-754 binary representation of `num`
pub fn f64_to_binary(num: &f64) -> String {
    // decompose num into component parts
    let (sign, exponent, mantissa) = num.decompose_raw();

    // convert each component into strings
    let sign_string = (sign as i64).to_string();
    let mantissa_string = format!("{:052b}", mantissa);
    let exponent_string = format!("{:011b}", exponent);

    // join component strings
    let binary_string = vec![sign_string, exponent_string, mantissa_string].join("");

    // return string representation
    return binary_string;
}

/// Converts String of length 64 to f64, yielding the floating-point number represented by the String
///
/// # Arguments
/// * `binary_string`: String showing IEEE-754 binary representation of a number
///
/// # Return
/// * `num`: f64 version of the String
pub fn binary_to_f64(binary_string: &String) -> f64 {
    // get sign and convert to bool as recompose expects
    let sign = &binary_string[0..1];
    let sign_bool = if sign.parse::<i32>().unwrap() == 0 {
        false
    } else {
        true
    };

    // convert exponent to int
    let exponent = &binary_string[1..12];
    let exponent_int = u16::from_str_radix(exponent, 2).unwrap();

    // convert mantissa to int
    let mantissa = &binary_string[12..];
    let mantissa_int = u64::from_str_radix(mantissa, 2).unwrap();

    // combine elements into f64 and return
    let num = f64::recompose_raw(sign_bool, exponent_int, mantissa_int);
    return num;
}

/// Takes 64-bit binary string and splits into sign, exponent, and mantissa
///
/// # Arguments
/// * `binary_string` - 64-bit binary string
///
/// # Return
/// * `(sign, exponent, mantissa)` - where each is a string
pub fn split_ieee_into_components(binary_string: &String) -> (String, String, String) {
    return (binary_string[0..1].to_string(), binary_string[1..12].to_string(), binary_string[12..].to_string());
}

/// Combines string versions of sign, exponent, and mantissa into single IEEE representation
///
/// # Arguments
/// * `sign` - Sign bit (length 1)
/// * `exponent` - Exponent bits (length 11)
/// * `mantissa` - Mantissa bits (length 52)
///
/// # Return
/// * `combined_string` - concatenation of sign, exponent, and mantissa
pub fn combine_components_into_ieee(sign: &str, exponent: &str, mantissa: &str) -> String {
    return vec![sign, exponent, mantissa].join("");
}

/// Samples a single element from a set according to provided weights
///
/// # Arguments
/// * `candidate_set` - The set from which you want to sample
/// * `weights` - Sampling weights for each element
///
/// # Return
/// Element from the candidate set
pub fn sample_from_set<T>(candidate_set: &Vec<T>, weights: &Vec<f64>)
    -> Result<T> where T: Clone {
    // generate uniform random number on [0,1)
    let unif: rug::Float = Float::with_val(53, noise::mpfr_uniform(0., 1.)?);

    // generate sum of weights
    let weights_rug: Vec<rug::Float> = weights.into_iter().map(|w| Float::with_val(53, w)).collect();
    let weights_sum: rug::Float = Float::with_val(53, Float::sum(weights_rug.iter()));

    // NOTE: use this instead of the two lines above if we switch to accepting rug::Float rather than f64 weights
    // let weights_sum: rug::Float = Float::with_val(53, Float::sum(weights.iter()));

    // convert weights to probabilities
    let probabilities: Vec<rug::Float> = weights_rug.iter().map(|w| w / weights_sum.clone()).collect();

    // generate cumulative probability distribution
    let mut cumulative_probability_vec: Vec<rug::Float> = Vec::with_capacity(weights.len() as usize);
    for i in 0..weights.len() {
        cumulative_probability_vec.push( Float::with_val(53, Float::sum(probabilities[0..(i+1)].iter())) );
    }

    // sample an element relative to its probability
    let mut return_index = 0;
    for i in 0..cumulative_probability_vec.len() {
        if unif <= cumulative_probability_vec[i] {
            return_index = i;
            break;
        }
    }
    Ok(candidate_set[return_index as usize].clone())
}

///  Accepts an ndarray and returns the number of columns
///
/// # Arguments
/// * `data` - The data for which you want to know the number of columns
///
/// # Return
/// Number of columns in data
pub fn get_num_columns<T>(data: &ArrayD<T>) -> Result<i64> {
    match data.ndim() {
        0 => Err("data is a scalar".into()),
        1 => Ok(1),
        2 => Ok(data.shape().last().unwrap().clone() as i64),
        _ => Err("data may be at most 2-dimensional".into())
    }
}
