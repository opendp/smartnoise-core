use openssl::rand::rand_bytes;
use std::{cmp, convert::TryFrom};
use ieee754::Ieee754;

use crate::utilities::utilities;

/// Gets smallest power of two that is equal to or greater than x.
///
/// # Arguments
/// * `x` - The number for which we want the next power of two.
///
/// # Returns
/// The number greater than x and the power of two it represents.
pub fn get_smallest_greater_or_eq_power_of_two(x: &f64) -> (f64, i64) {
    // convert x to binary and split it into its component parts
    let x_binary = utilities::f64_to_binary(&x);
    let (sign, exponent, mantissa) = utilities::split_ieee_into_components(&x_binary);

    // build string of all zeros to be used later
    let mut all_zeros = String::with_capacity(52);
    for _ in 0..52 {
        all_zeros.push_str("0");
    }
    if mantissa == all_zeros {
        // if mantissa is all zeros, then x is already a power of two
        return(*x, i64::from_str_radix(&exponent, 2).unwrap() - 1023);
    } else {
        // otherwise, convert the mantissa to all zeros and increment the exponent
        let exponent_plus_one_int = i64::from_str_radix(&exponent, 2).unwrap() + 1;
        let exponent_plus_one_bin = format!("{:011b}", exponent_plus_one_int);
        let greater_or_eq_power_of_two_bin = utilities::combine_components_into_ieee(&sign, &exponent_plus_one_bin, &all_zeros);
        let greater_or_eq_power_of_two_f64 = utilities::binary_to_f64(&greater_or_eq_power_of_two_bin);
        return(greater_or_eq_power_of_two_f64, exponent_plus_one_int-1023);
    }
}

/// Accepts components of IEEE string and `power`, divides the exponent by `power`, and returns the updated components.
///
/// # Arguments
/// * `sign` - Sign bit (length 1).
/// * `exponent` - Exponent bits (length 11).
/// * `mantissa` - Mantissa bits (length 52).
/// * `power` - Power of two by which components should be divided. 
///
/// # Return
/// Updated components - sign, updated exponent, and mantissa.
pub fn divide_components_by_power_of_two(sign: &str, exponent: &str, mantissa: &str, power: &i64) -> (String, String, String) {
    // update exponent by subtracting power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() - power;
    let updated_exponent_bin = format!("{:011b}", updated_exponent_int);

    // return components
    return(sign.to_string(), updated_exponent_bin.to_string(), mantissa.to_string());
}

/// Accepts components of IEEE string and `power`, multiplies the exponent by `power`, and returns the updated components.
///
/// # Arguments
/// * `sign` - Sign bit (length 1).
/// * `exponent` - Exponent bits (length 11).
/// * `mantissa` - Mantissa bits (length 52).
/// * `power` - Power of two by which components should be multiplied. 
///
/// # Return
/// Updated components - sign, updated exponent, and mantissa.
pub fn multiply_components_by_power_of_two(sign: &str, exponent: &str, mantissa: &str, power: &i64) -> (String, String, String) {
    // update exponent by subtracting power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() + power;
    let updated_exponent_bin = format!("{:011b}", updated_exponent_int);

    // return components
    return(sign.to_string(), updated_exponent_bin.to_string(), mantissa.to_string());
}

/// Accepts components of IEEE representation, rounds to the nearest integer, and returns updated components.
///
/// # Arguments
/// * `sign` - Sign bit (length 1).
/// * `exponent` - Exponent bits (length 11).
/// * `mantissa` - Mantissa bits (length 52).
///
/// Returns
/// Updated components - sign, exponent, and mantissa.
pub fn round_components_to_nearest_int(sign: &str, exponent: &str, mantissa: &str) -> (String, String, String) {
    // get unbiased exponent
    let unbiased_exponent_numeric_i64 = i64::from_str_radix(&exponent, 2).unwrap() - 1023;
    let unbiased_exponent_numeric:usize = if unbiased_exponent_numeric_i64 > 0 { usize::try_from(unbiased_exponent_numeric_i64).unwrap()} else { 0 };

    // let unbiased_exponent_numeric = usize::try_from(unbiased_exponent_numeric_i64).unwrap();
    println!("unbiased exponent numeric: {}", unbiased_exponent_numeric);

    // build strings of all zeros and ones to be used later
    let mut all_zeros = String::with_capacity(unbiased_exponent_numeric);
    let mut all_ones = String::with_capacity(unbiased_exponent_numeric);
    for _ in 0..unbiased_exponent_numeric {
        all_zeros.push_str("0");
        all_ones.push_str("1");
    }

    // return original components if they already represent an integer, otherwise proceed
    if unbiased_exponent_numeric_i64 >= 52 {
        return(sign.to_string(), exponent.to_string(), mantissa.to_string());
    } else if unbiased_exponent_numeric_i64 >= 0 {
        // get elements of mantissa that represent integers (after being multiplied by 2^unbiased_exponent_num)
        let mantissa_subset:String = mantissa[0..unbiased_exponent_numeric].into();
        println!("mantissa_subset: {}", mantissa_subset);

        // check to see if mantissa needs to be rounded up or down
        // if mantissa needs to be rounded up ...
        if mantissa[unbiased_exponent_numeric..unbiased_exponent_numeric+1] == *"1" {
            // if integer part of mantissa is all 1s, rounding needs to be reflected in the exponent instead
            if mantissa_subset == all_ones {
                println!("rounding up exponent");
                let exponent_increased_numeric = i64::from_str_radix(&exponent, 2).unwrap() + 1;
                let exponent_increased_bin = format!("{:011b}", exponent_increased_numeric);
                return(sign.to_string(), exponent_increased_bin.to_string(), format!("{:0<52}", "0"));
            } else {
                println!("rounding up mantissa");
                // if integer part of mantissa not all 1s, just increment mantissa
                let mantissa_subset_increased_numeric = u64::from_str_radix(&mantissa_subset, 2).unwrap() + 1;
                let mantissa_subset_increased_bin = format!("{:0>width$b}", mantissa_subset_increased_numeric, width = unbiased_exponent_numeric);
                let mantissa_increased_bin = format!("{:0<52}", mantissa_subset_increased_bin); // append zeros to right
                return(sign.to_string(), exponent.to_string(), mantissa_increased_bin.to_string());
            }
        } else {
            // mantissa needs to be rounded down
            println!("rounding down mantissa");
            return(sign.to_string(), exponent.to_string(), format!("{:0<52}", mantissa_subset));
        }
    } else {
        // if unbiased_exponent_numeric < 0
        // let unbiased_exponent_numeric_i64 = unbiased_exponent_numeric as i64;
        if unbiased_exponent_numeric_i64 == -1 {
            // round int to +- 1
            println!("rounding to +- 1");
            let exponent_for_one = format!("{:1<11}", "0");
            return(sign.to_string(), exponent_for_one.to_string(), format!("{:0<52}", "0"));
        } else {
            // round int to 0
            println!("rounding to 0");
            let exponent_for_zero = format!("{:0>11}", "0");
            return(sign.to_string(), exponent_for_zero.to_string(), format!("{:0<52}", "0"));
        }
    }
}

/// Finds the closest number to x that is a multiple of Lambda.
/// 
/// # Arguments
/// * `x` - Number to be rounded to closest multiple of Lambda.
/// * `m` - Integer such that Lambda = 2^m.
///
/// # Returns
/// Closest multiple of Lambda to x.
pub fn get_closest_multiple_of_Lambda(x: &f64, m: &i64) -> f64 {
    let x_binary = utilities::f64_to_binary(&x);
    let (sign_a, exponent_a, mantissa_a) = utilities::split_ieee_into_components(&x_binary);
    let (sign_b, exponent_b, mantissa_b) = divide_components_by_power_of_two(&sign_a, &exponent_a, &mantissa_a, &m);
    let (sign_c, exponent_c, mantissa_c) = round_components_to_nearest_int(&sign_b, &exponent_b, &mantissa_b);
    let (sign_d, exponent_d, mantissa_d) = multiply_components_by_power_of_two(&sign_c, &exponent_c, &mantissa_c, &m);
    let Lambda_mult_binary = utilities::combine_components_into_ieee(&sign_d, &exponent_d, &mantissa_d);
    let Lambda_mult_f64 = utilities::binary_to_f64(&Lambda_mult_binary);
    return Lambda_mult_f64;
}

/// Gets functional epsilon for Snapping mechanism such that privacy loss does not exceed the user's proposed budget.
/// Described in https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
/// 
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `B` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Functional epsilon that will determine amount of noise.
pub fn redefine_epsilon(epsilon: &f64, B: &f64, precision: &u32) -> f64 {
    let eta = 2_f64.powf(-(*precision as f64));
    return (epsilon - 2.0*eta) / (1.0 + 12.0*B*eta);
}

/// Finds accuracy that is achievable given desired epsilon and confidence requirements. Described in
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `alpha` - Desired confidence level.
/// * `epsilon` - Desired privacy guarantee.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `B` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn get_accuracy(alpha: &f64, epsilon: &f64, sensitivity: &f64, B: &f64, precision: &u32) -> f64 {
    let accuracy = ( (1.0 + 12.0 * B * 2_f64.powf(-(*precision as f64))) / (epsilon - 2_f64.powf(-(*precision as f64) + 1.)) )
                   * (1.0 + (1.0 / alpha).ln()) * (sensitivity);
    return accuracy;
}

/// Finds epsilon that will achieve desired accuracy and confidence requirements. Described in 
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `accuracy` - Desired accuracy level.
/// * `alpha` - Desired confidence level.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `B` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn get_epsilon(accuracy: &f64, alpha: &f64, sensitivity: &f64, B: &f64, precision: &u32) -> f64 {
    let epsilon = ( (1.0 + 12.0 * B * 2_f64.powf(-(*precision as f64))) / accuracy) * (1.0 + (1.0 / alpha).ln())
                  * (sensitivity) + 2_f64.powf(-(*precision as f64) + 1.);
    return epsilon;
}

pub fn get_precision(B: &f64) -> u32 {
    let precision: u32;
    if (B <= &(2_u32.pow(66) as f64)) {
        precision = 118;
    } else {
        let (t, k) = get_smallest_greater_or_eq_power_of_two(&B);
        precision = 118 + (k as u32) - 66;
    }
    return precision;
}

/// Given input parameters, finds values of parameters for use inside of mechanism
/// (e.g. scaled bounds, epsilon_prime to set the inner noise distribution, etc.)
/// 
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `B` - Upper bound on function value being privatized.
/// * `sensitivity` - l1 sensitivity for function to which the mechanism is being applied.
///
/// # Returns
/// Updated parameters for the Snapping mechanism.
pub fn parameter_setup(epsilon: &f64, B: &f64, sensitivity: &f64) -> (f64, f64, f64, f64, i64, u32) {
    // find sufficient precision
    let precision = get_precision(&B);

    // scale clamping bound by sensitivity
    let B_scaled = B / sensitivity;
    let epsilon_prime = redefine_epsilon(&epsilon, &B_scaled, &precision);

    // NOTE: this Lambda is calculated relative to lambda = 1/epsilon' rather than sensitivity/epsilon' because we have already
    //       scaled by the sensitivity
    let lambda_prime_scaled = 1.0/epsilon_prime;
    let (Lambda_prime_scaled, m) = get_smallest_greater_or_eq_power_of_two(&lambda_prime_scaled);
    let Lambda_prime = Lambda_prime_scaled * sensitivity;

    return(B_scaled, epsilon_prime, Lambda_prime, Lambda_prime_scaled, m, precision);
}