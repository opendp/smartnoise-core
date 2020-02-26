use yarrow_validator::errors::*;
use yarrow_validator::ErrorKind::{PrivateError, PublicError};

use openssl::rand::rand_bytes;
use ieee754::Ieee754;

use crate::utilities::noise;

pub fn get_bytes(n_bytes: usize) -> String {
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

pub fn f64_to_binary(num: &f64) -> String {
    /// Converts f64 to String of length 64, yielding the IEEE-754 binary representation of the number
    ///
    /// # Arguments
    /// * `num` - a number of type f64
    ///
    /// # Return
    /// * `binary_string`: String showing IEEE-754 binary representation of `num`


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

pub fn binary_to_f64(binary_string: &String) -> f64 {
    /// Converts String of length 64 to f64, yielding the floating-point number represented by the String
    ///
    /// # Arguments
    /// * `binary_string`: String showing IEEE-754 binary representation of a number
    ///
    /// # Return
    /// * `num`: f64 version of the String

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

pub fn split_ieee_into_components(binary_string: &String) -> (String, String, String) {
    /// Takes 64-bit binary string and splits into sign, exponent, and mantissa
    ///
    /// # Arguments
    /// * `binary_string` - 64-bit binary string
    ///
    /// # Return
    /// * `(sign, exponent, mantissa)` - where each is a string
    return(binary_string[0..1].to_string(), binary_string[1..12].to_string(), binary_string[12..].to_string());
}

pub fn combine_components_into_ieee(sign: &str, exponent: &str, mantissa: &str) -> String {
    /// Combines string versions of sign, exponent, and mantissa into single IEEE representation
    ///
    /// # Arguments
    /// * `sign` - Sign bit (length 1)
    /// * `exponent` - Exponent bits (length 11)
    /// * `mantissa` - Mantissa bits (length 52)
    ///
    /// # Return
    /// * `combined_string` - concatenation of sign, exponent, and mantissa
    let combined_string = vec![sign, exponent, mantissa].join("");
    return combined_string;
}

pub fn sample_from_set<T>(candidate_set: &Vec<T>, weights: &Vec<f64>) -> T where T: Clone, {
    // generate uniform random number on [0,1)
    let unif: f64 = noise::sample_uniform(0., 1.);

    // generate sum of weights
    let weights_sum: f64 = weights.iter().sum();

    // convert weights to probabilities
    let probabilities: Vec<f64> = weights.iter().map(|w| w / weights_sum).collect();

    // generate cumulative probability distribution
    let cumulative_probability_vec = probabilities.iter().scan(0.0, |sum, i| {*sum += i; Some(*sum)}).collect::<Vec<_>>();

    // sample an element relative to its probability
    let mut return_index = 0;
    for i in 0..cumulative_probability_vec.len() {
        if unif <= cumulative_probability_vec[i] {
            return_index = i;
            break
        }
    }
    return candidate_set[return_index as usize].clone()
}