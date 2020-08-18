use whitenoise_validator::errors::*;

use crate::utilities;

/// Gets smallest power of two that is equal to or greater than x.
///
/// # Arguments
/// * `x` - The number for which we want the next power of two.
///
/// # Returns
/// The number greater than x and the power of two it represents.
pub fn get_smallest_greater_or_eq_power_of_two(x: f64) -> Result<(f64, i64)> {
    // convert x to binary and split it into its component parts
    let x_binary = utilities::f64_to_binary(x);
    let (sign, exponent, mantissa) = utilities::split_ieee_into_components(x_binary);

    // build string of all zeros to be used later
    let all_zeros = "0".repeat(52);
    Ok(if mantissa == all_zeros {
        // if mantissa is all zeros, then x is already a power of two
        (x, i64::from_str_radix(&exponent, 2).unwrap() - 1023)
    } else {
        // otherwise, convert the mantissa to all zeros and increment the exponent
        let exponent_plus_one_int = i64::from_str_radix(&exponent, 2).unwrap() + 1;
        let exponent_plus_one_bin = format!("{:011b}", exponent_plus_one_int);
        let greater_or_eq_power_of_two_bin = utilities::combine_components_into_ieee((sign, exponent_plus_one_bin, all_zeros));
        let greater_or_eq_power_of_two_f64 = utilities::binary_to_f64(&greater_or_eq_power_of_two_bin)?;
        (greater_or_eq_power_of_two_f64, exponent_plus_one_int - 1023)
    })
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
pub fn divide_components_by_power_of_two(
    (sign, exponent, mantissa): (String, String, String), power: i64,
) -> (String, String, String) {

    // update exponent by subtracting power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() - power;

    // return components
    (sign.to_string(), format!("{:011b}", updated_exponent_int.max(0)), mantissa.to_string())
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
/// Updated components: sign, updated exponent, and mantissa.
pub fn multiply_components_by_power_of_two(
    (sign, exponent, mantissa): (String, String, String), power: i64
) -> (String, String, String) {
    // update exponent by adding power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() + power;
    // return components
    (sign.to_string(), format!("{:011b}", updated_exponent_int.max(0)), mantissa.to_string())
}

/// Accepts components of IEEE representation, rounds to the nearest integer, and returns updated components.
///
/// # Arguments
/// * `sign` - Sign bit (length 1).
/// * `exponent` - Exponent bits (length 11).
/// * `mantissa` - Mantissa bits (length 52).
///
/// Returns
/// Updated components: sign, exponent, and mantissa.
pub fn round_components_to_nearest_int(
    (sign, exponent, mantissa): (String, String, String)
) -> (String, String, String) {
    // get unbiased exponent
    let unbiased_exponent_numeric = i64::from_str_radix(&exponent, 2).unwrap() - 1023;

    match unbiased_exponent_numeric {
        // original components already represent an integer
        i if i >= 52 =>
            (sign.to_string(), exponent.to_string(), mantissa.to_string()),
        // round int to +- 1
        i if i == -1 =>
            (sign.to_string(), format!("{:1<11}", "0").to_string(), format!("{:0<52}", "0")),
        // round int to 0
        i if i < -1 =>
            (sign.to_string(), format!("{:0>11}", "0"), format!("{:0<52}", "0")),
        _ => {
            let unbiased_exponent_numeric = unbiased_exponent_numeric as usize;

            // let unbiased_exponent_numeric = usize::try_from(unbiased_exponent_numeric_i64).unwrap();
            // println!("unbiased exponent numeric: {}", unbiased_exponent_numeric);

            // get elements of mantissa that represent integers (after being multiplied by 2^unbiased_exponent_num)
            let mantissa_subset: String = mantissa[0..unbiased_exponent_numeric].into();
            // println!("mantissa_subset: {}", mantissa_subset);

            // check to see if mantissa needs to be rounded up or down
            // if mantissa needs to be rounded up ...
            if mantissa[unbiased_exponent_numeric..unbiased_exponent_numeric + 1] == *"1" {
                // if integer part of mantissa is all 1s, rounding needs to be reflected in the exponent instead
                if mantissa_subset == "1".repeat(unbiased_exponent_numeric) {
                    // println!("rounding up exponent");
                    let exponent_increased_numeric = i64::from_str_radix(&exponent, 2).unwrap() + 1;
                    let exponent_increased_bin = format!("{:011b}", exponent_increased_numeric);
                    (sign.to_string(), exponent_increased_bin.to_string(), format!("{:0<52}", "0"))
                } else {
                    // println!("rounding up mantissa");
                    // if integer part of mantissa not all 1s, just increment mantissa
                    let mantissa_subset_increased_numeric = u64::from_str_radix(&mantissa_subset, 2).unwrap() + 1;
                    let mantissa_subset_increased_bin = format!("{:0>width$b}", mantissa_subset_increased_numeric, width = unbiased_exponent_numeric);
                    let mantissa_increased_bin = format!("{:0<52}", mantissa_subset_increased_bin); // append zeros to right
                    (sign.to_string(), exponent.to_string(), mantissa_increased_bin.to_string())
                }
            } else {
                // mantissa needs to be rounded down
                // println!("rounding down mantissa");
                (sign.to_string(), exponent.to_string(), format!("{:0<52}", mantissa_subset))
            }
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
pub fn get_closest_multiple_of_lambda(x: f64, m: i64) -> Result<f64> {
    let x_binary = utilities::f64_to_binary(x);
    let components = utilities::split_ieee_into_components(x_binary);
    let components = divide_components_by_power_of_two(components, m);
    let components = round_components_to_nearest_int(components);
    let components = multiply_components_by_power_of_two(components, m);
    let lambda_mult_binary = utilities::combine_components_into_ieee(components);
    utilities::binary_to_f64(&lambda_mult_binary)
}


#[test]
fn test_get_closest_multiple_of_lambda() {
    (0..100).for_each(|i| {
        let x = 1. - 0.01 * (i as f64);
        println!("{}: {}", x, get_closest_multiple_of_lambda(x, -1).unwrap())
    });
}

/// Gets functional epsilon for Snapping mechanism such that privacy loss does not exceed the user's proposed budget.
/// Described in https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
/// 
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `b` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Functional epsilon that will determine amount of noise.
pub fn redefine_epsilon(epsilon: f64, b: f64, precision: u32) -> f64 {
    let eta = 2_f64.powf(-(precision as f64));
    (epsilon - 2.0 * eta) / (1.0 + 12.0 * b * eta)
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
pub fn get_accuracy(alpha: &f64, epsilon: &f64, sensitivity: &f64, b: &f64, precision: &u32) -> f64 {
    ((1.0 + 12.0 * b * 2_f64.powf(-(*precision as f64))) / (epsilon - 2_f64.powf(-(*precision as f64) + 1.)))
        * (1.0 + (1.0 / alpha).ln()) * (sensitivity)
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
pub fn get_snapping_epsilon(accuracy: &f64, alpha: &f64, sensitivity: &f64, b: &f64, precision: &u32) -> f64 {
    let epsilon = ((1.0 + 12.0 * b * 2_f64.powf(-(*precision as f64))) / accuracy) * (1.0 + (1.0 / alpha).ln())
        * (sensitivity) + 2_f64.powf(-(*precision as f64) + 1.);
    return epsilon;
}

/// Finds precision necessary to run Snapping mechanism.
/// 
/// # Arguments
/// * `b` - Upper bound on function value being privatized.
/// 
/// # Returns
/// Gets necessary precision for Snapping mechanism.
pub fn get_precision(b: f64) -> Result<u32> {
    Ok(if b <= 2_u128.pow(66) as f64 {
        118
    } else {
        let (_t, k) = get_smallest_greater_or_eq_power_of_two(b)?;
        118 + (k as u32) - 66
    })
}

pub struct SnappingConfig {
    pub b_scaled: f64,
    pub epsilon_prime: f64,
    pub lambda_prime: f64,
    pub lambda_prime_scaled: f64,
    pub m: i64,
    pub precision: u32
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
pub fn parameter_setup(epsilon: f64, b: f64, sensitivity: f64) -> Result<SnappingConfig> {
    // find sufficient precision
    let precision = get_precision(b)?;

    // scale clamping bound by sensitivity
    let b_scaled = b / sensitivity;
    let epsilon_prime = redefine_epsilon(epsilon, b_scaled, precision);

    // NOTE: this Lambda is calculated relative to lambda = 1/epsilon' rather than sensitivity/epsilon'
    //    because we have already scaled by the sensitivity
    let (lambda_prime_scaled, m) = get_smallest_greater_or_eq_power_of_two(1.0 / epsilon_prime)?;
    let lambda_prime = lambda_prime_scaled * sensitivity;

    Ok(SnappingConfig {
        b_scaled,
        epsilon_prime,
        lambda_prime,
        lambda_prime_scaled,
        m,
        precision
    })
}