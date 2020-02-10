use openssl::rand::rand_bytes;
use std::{cmp, convert::TryFrom};
use ieee754::Ieee754;

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

pub fn get_smallest_greater_or_eq_power_of_two(x: &f64) -> (f64, i64) {
    /// Gets smallest power of two that is equal to or greater than x
    ///
    /// # Arguments
    /// * `x` - the number for which we want the next power of two
    ///
    /// # Returns
    /// * (f64 greater power of two value, i64 of the actual power)


    // convert x to binary and split it into its component parts
    let x_binary = f64_to_binary(&x);
    let (sign, exponent, mantissa) = split_ieee_into_components(&x_binary);

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
        let greater_or_eq_power_of_two_bin = combine_components_into_ieee(&sign, &exponent_plus_one_bin, &all_zeros);
        let greater_or_eq_power_of_two_f64 = binary_to_f64(&greater_or_eq_power_of_two_bin);
        return(greater_or_eq_power_of_two_f64, exponent_plus_one_int-1023);
    }
}

pub fn divide_components_by_power_of_two(sign: &str, exponent: &str, mantissa: &str, power: &i64) -> (String, String, String) {
    /// Accepts components of IEEE string and `power`, divides the exponent by `power`, and returns the updated components
    ///
    /// # Arguments
    /// * `sign` - Sign bit (length 1)
    /// * `exponent` - Exponent bits (length 11)
    /// * `mantissa` - Mantissa bits (length 52)
    ///
    /// # Return
    /// * updated components - sign, updated exponent, and mantissa

    // update exponent by subtracting power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() - power;
    let updated_exponent_bin = format!("{:011b}", updated_exponent_int);

    // return components
    return(sign.to_string(), updated_exponent_bin.to_string(), mantissa.to_string());
}

pub fn multiply_components_by_power_of_two(sign: &str, exponent: &str, mantissa: &str, power: &i64) -> (String, String, String) {
    /// Accepts components of IEEE string and `power`, multiplies the exponent by `power`, and returns the updated components
    ///
    /// # Arguments
    /// * `sign` - Sign bit (length 1)
    /// * `exponent` - Exponent bits (length 11)
    /// * `mantissa` - Mantissa bits (length 52)
    ///
    /// # Return
    /// * updated components - sign, updated exponent, and mantissa

    // update exponent by subtracting power, then convert back to binary
    let updated_exponent_int = i64::from_str_radix(&exponent, 2).unwrap() + power;
    let updated_exponent_bin = format!("{:011b}", updated_exponent_int);

    // return components
    return(sign.to_string(), updated_exponent_bin.to_string(), mantissa.to_string());
}

pub fn round_components_to_nearest_int(sign: &str, exponent: &str, mantissa: &str) -> (String, String, String) {
    /// Accepts components of IEEE representation, rounds to the nearest integer, and returns updated components
    ///
    /// # Arguments
    /// * `sign` - Sign bit (length 1)
    /// * `exponent` - Exponent bits (length 11)
    /// * `mantissa` - Mantissa bits (length 52)
    ///
    /// Returns
    /// * updated components - sign, exponent, and mantissa

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

pub fn get_closest_multiple_of_Lambda(x: &f64, m: &i64) -> f64 {
    ///
    /// # Arguments
    /// * `x` - number to be rounded to closest multiple of Lambda
    /// * `m` - integer such that Lambda = 2^m
    ///
    /// # Returns
    /// closest multiple of Lambda to `x`

    let x_binary = f64_to_binary(&x);
    let (sign_a, exponent_a, mantissa_a) = split_ieee_into_components(&x_binary);
    let (sign_b, exponent_b, mantissa_b) = divide_components_by_power_of_two(&sign_a, &exponent_a, &mantissa_a, &m);
    let (sign_c, exponent_c, mantissa_c) = round_components_to_nearest_int(&sign_b, &exponent_b, &mantissa_b);
    let (sign_d, exponent_d, mantissa_d) = multiply_components_by_power_of_two(&sign_c, &exponent_c, &mantissa_c, &m);
    let Lambda_mult_binary = combine_components_into_ieee(&sign_d, &exponent_d, &mantissa_d);
    let Lambda_mult_f64 = binary_to_f64(&Lambda_mult_binary);
    return Lambda_mult_f64;
}

pub fn redefine_epsilon(epsilon: &f64, B: &f64, precision: &f64) -> f64 {
    /// Redefine epsilon for snapping mechanism such that we can
    /// ensure that we do not exhaust too much privacy budget
    ///
    /// # Arguments
    /// * `epsilon` - desired privacy guarantee
    /// * `B` - snapping bound
    /// * `precision` - amount of arithmetic precision to which we have access
    ///
    /// # Returns
    /// functional epsilon that will determine amount of noise

    let eta = 2_f64.powf(-precision);
    return (epsilon - 2.0*eta) / (1.0 + 12.0*B*eta);
}

pub fn get_accuracy(alpha: &f64, epsilon: &f64, sensitivity: &f64, B: &f64, precision: &f64) -> f64 {
    /// Get accuracy as described in
    /// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
    ///
    /// # Arguments
    /// * `alpha` - desired confidence level
    /// * `epsilon` - desired privacy guarantee
    /// * `sensitivity` - sensitivity for function to which mechanism is being applied
    /// * `B` - snapping bound
    /// * `precision` - amount of arithmetic precision to which we have access
    ///
    /// # Returns
    /// accuracy guarantee for snapping mechanism

    let accuracy = ( (1.0 + 12.0 * B * 2_f64.powf(-precision)) / (epsilon - 2_f64.powf(-precision + 1.0)) )
                   * (1.0 + (1.0 / alpha).ln()) * (sensitivity);
    return accuracy;
}

pub fn get_epsilon(accuracy: &f64, alpha: &f64, sensitivity: &f64, B: &f64, precision: &f64) -> f64 {
    /// Given accuracy, get epsilon as described in
    /// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
    ///
    /// # Arguments
    /// * `accuracy` - desired accuracy level
    /// * `alpha` - desired confidence level
    /// * `sensitivity` - sensitivity for function to which mechanism is being applied
    /// * `B` - snapping bound
    /// * `precision` - amount of arithmetic precision to which we have access
    ///
    /// # Returns
    /// epsilon use for snapping mechanism

    let epsilon = ( (1.0 + 12.0 * B *2_f64.powf(-precision)) / accuracy) * (1.0 + (1.0 / alpha).ln())
                  * (sensitivity) + 2_f64.powf(-precision + 1.0);
    return epsilon;
}

pub fn parameter_setup(epsilon: &f64, B: &f64, sensitivity: &f64, precision: &f64) -> (f64, f64, f64, f64, i64) {
    /// Given input parameters, finds values of parameters for use inside of mechanism
    /// (e.g. scaled bounds, epsilon_prime to set the inner noise distribution, etc.)
    /// # Arguments
    /// * `epsilon` - desired privacy guarantee
    /// * `B` - snapping bound
    /// * `sensitivity` - sensitivity for function to which mechanism is being applied
    /// * `precision` - amount of arithmetic precision to which we have access
    ///
    /// # Returns
    /// updated parameters for snapping mechanism

    // scale clamping bound by sensitivity
    let B_scaled = B / sensitivity;
    let epsilon_prime = redefine_epsilon(&epsilon, &B_scaled, &precision);

    // NOTE: this Lambda is calculated relative to lambda = 1/epsilon' rather than sensitivity/epsilon' because we have already
    //       scaled by the sensitivity
    let lambda_prime_scaled = 1.0/epsilon_prime;
    let (Lambda_prime_scaled, m) = get_smallest_greater_or_eq_power_of_two(&lambda_prime_scaled);
    let Lambda_prime = Lambda_prime_scaled * sensitivity;

    return(B_scaled, epsilon_prime, Lambda_prime, Lambda_prime_scaled, m);
}