use std::num::ParseIntError;
use whitenoise_validator::{Float, Integer};
use permutohedron::heap_recursive;
use crate::components::linreg_error::Error;
use rand::prelude::*;
use rand::{seq::IteratorRandom, thread_rng}; // 0.6.1

pub fn all_permutations(mut vec: Vec<Integer>, k: Integer) -> Vec<Vec<Integer>> {
    let mut permutations = Vec::new();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = vec.choose_multiple(&mut rng, k as usize).cloned().collect();
    heap_recursive(&mut vec_sample, |permutation| {
        permutations.push(permutation.to_vec())
    });
    permutations
}

pub fn permute_range(n: Integer, k: Integer) -> Vec<Integer> {
    let mut range = (1..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, k as usize).cloned().collect();
    vec_sample.shuffle(&mut rng);
    vec_sample
}

pub fn dp_med(z: Vec<Float>, epsilon: Float, n: Integer, k: Integer) -> Float {
    return Float::from(1.0);
}

pub fn dp_theil_sen_k_match(x: Vec<Float>, y: Vec<Float>, n: Integer, k: Integer, epsilon: Float) -> Result<(Float, Float), Error> {
    let mut z_25 = Vec::new();
    let mut z_75 = Vec::new();

    let tau = permute_range(n, k);

    // For sampling without replacement, shuffle this list and draw first (or last) element
    let mut h_vec = (1..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = thread_rng();
    h_vec.shuffle(&mut rng);

    for i in 0..k-1 {
        let j = tau[i as usize] as usize;
        let l = tau[i as usize + 1 as usize] as usize;
        if x[l] - x[j] != 0.0 {
            let slope = (y[l] - y[j]) / (x[l] - x[j]);
            z_25.push(slope * (0.25 - (x[l] + x[j])/2.0) + (y[l] + y[j])/2.0);
            z_75.push(slope * (0.75 - (x[l] + x[j])/2.0) + (y[l] + y[j])/2.0);
        } else {
            Error::TooSteep;
        }
    }
    let pfinal_25 = dp_med(z_25, epsilon, n, k);
    let pfinal_75 = dp_med(z_75, epsilon, n, k);

    Ok((pfinal_25, pfinal_75))

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permute_test_values_of_k() {
        let v = vec![1,2,3,4,5,6];
        assert_eq!(all_permutations(v, 2).len(), 2);
        let v = vec![1,2,3,4,5,6];
        assert_eq!(all_permutations(v, 4).len(), 24);
        let v = vec![1,2,3,4,5,6];
        assert_eq!(all_permutations(v, 5).len(), 120);
        let v = vec![1,2,3,4,5,6];
        assert_eq!(all_permutations(v, 6).len(), 720);
    }

    #[test]
    fn dp_theilsen_test() {
        let x = vec![0.0, 1.0, 2.0, 3.0, 4.0];
        let y = vec![0.0, 2.0, 4.0, 6.0, 8.0];
        assert_eq!((1.0, 1.0), dp_theil_sen_k_match(x, y, 4, 2, 0.1).unwrap());
    }
}