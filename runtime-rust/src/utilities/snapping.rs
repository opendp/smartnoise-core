use openssl::rand::rand_bytes;
use std::cmp;
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

pub fn get_geom_prob_one_half() -> i16 {
    /// Return sample from a truncated Geometric distribution with parameter p=0.5
    ///
    /// The algorithm generates 1024 bits uniformly at random and returns the
    /// index of the first bit with value 1. If all 1024 bits are 0, then
    /// the algorithm acts as if the last bit was a 1 and returns 1024.
    ///
    /// This method was written specifically to generate the exponent
    /// that will be used for the uniform random number generation
    /// embedded within the Snapping Mechanism.
    ///

    let mut geom: (i16) = 1024;
    // read bytes in one at a time, need 128 to fully generate geometric
    for i in 0..128 {
        // read random bytes
        let binary_string = get_bytes(1);
        let binary_char_vec: Vec<char> = binary_string.chars().collect();

        // find first element that is '1' and mark its overall index
        let first_one_index = binary_char_vec.iter().position(|&x| x == '1');
        let first_one_overall_index: i16;
        if first_one_index.is_some() {
            let first_one_index_int = first_one_index.unwrap() as i16;
            first_one_overall_index = 8*i + first_one_index_int;
        } else {
            first_one_overall_index = 1024;
        }
        geom = cmp::min(geom, first_one_overall_index+1);
    }
    return geom;
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
    let (mut sign, mut exponent, mut mantissa) = num.decompose_raw();

    // convert each component into strings
    let sign_string = (sign as i64).to_string();
    let mantissa_string = format!("{:052b}", mantissa);
    let exponent_string = format!("{:011b}", exponent);

    // join component strings
    let mut binary_string = vec![sign_string, exponent_string, mantissa_string].join("");

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
    let (mut sign, mut exponent, mut mantissa) = split_ieee_into_components(&x_binary);

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