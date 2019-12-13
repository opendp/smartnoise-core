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