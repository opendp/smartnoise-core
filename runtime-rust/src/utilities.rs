use openssl::rand::rand_bytes;

extern crate byteorder;

pub fn sample_laplace(shift: f64, scale: f64) -> f64 {
    let sample = sample_uniform(0., 1.);

    match sample < shift {
        true => 0.5 * ((sample - shift).abs() / scale).exp(),
        false => 1. - 0.5 * (-(sample - shift).abs() / scale).exp()
    }
}

pub fn sample_uniform(min: f64, max: f64) -> f64 {
    let mut buf = [0; 8];
    rand_bytes(&mut buf).unwrap();

    let sample = f64::from_le_bytes(buf) % (max - min) + min;
    println!("uniform sample: {:?}", sample);
    sample
}