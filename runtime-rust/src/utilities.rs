use openssl::rand::rand_bytes;
use byteorder::{ByteOrder, LittleEndian};
use probability::distribution::{Gaussian, Laplace, Inverse};

pub fn sample_laplace(shift: f64, scale: f64) -> f64 {
    let sample = sample_uniform(0., 1.);
    Laplace::new(shift, scale).inverse(sample)
//    shift - scale * (sample - 0.5).signum() * (1. - 2. * (sample - 0.5).abs()).ln()
}

pub fn sample_gaussian(shift: f64, scale: f64) -> f64 {
    let sample = sample_uniform(0., 1.);
    Gaussian::new(shift, scale).inverse(sample)
}

pub fn sample_uniform(min: f64, max: f64) -> f64 {
    let mut buf = [0; 8];
    rand_bytes(&mut buf).unwrap();

    let sample = (LittleEndian::read_u64(&buf) as f64) / (std::u64::MAX as f64) * (max - min) + min;
    sample
}