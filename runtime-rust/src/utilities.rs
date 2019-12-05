use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse};
use std::cmp;
use ieee754::Ieee754;

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
    let binary_string = get_bytes(7);
    let mantissa = &binary_string[0..52];

    // convert mantissa to integer
    let mantissa_int = u64::from_str_radix(mantissa, 2).unwrap();

    // Generate exponent
    let geom: (i16) = get_geom_prob_one_half();
    let exponent: (u16) = (-geom + 1023) as u16;

    // Generate uniform random number from (0,1)
    let uniform_rand = f64::recompose_raw(false, exponent, mantissa_int);

    return uniform_rand;
}