use whitenoise_validator::{Float, Integer};
use permutohedron::heap_recursive;
use crate::components::linreg_error::Error;
use rand::prelude::*;
use rand::thread_rng;
use ndarray::{ArrayD, arr1};
use crate::components::clamp::clamp_numeric_float;
// use gmp_mpfr_sys::mpfr::log;
use crate::utilities::{noise};

pub fn all_permutations(vec: Vec<Integer>, n: Integer) -> Vec<Vec<Integer>> {
    let mut permutations = Vec::new();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = vec.choose_multiple(&mut rng, n as usize).cloned().collect();
    heap_recursive(&mut vec_sample, |permutation| {
        permutations.push(permutation.to_vec())
    });
    permutations
}

pub fn permute_range(n: Integer, k: Integer) -> Vec<Integer> {
    let range = (1..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, k as usize).cloned().collect();
    vec_sample.shuffle(&mut rng);
    vec_sample
}

pub fn compute_all_ests(x: Vec<Float>, y: Vec<Float>, n: Integer) -> Vec<Vec<Float>> {
    let mut estimates: Vec<Vec<Float>> = Vec::new();
    estimates.push(Vec::new());
    estimates.push(Vec::new());

    for p in 0..n {
        for q in p+1..n {
            let p: usize = p as usize;
            let q: usize = q as usize;
            let x_delta = x[q] - x[p];
            if x_delta != 0.0 {
                let slope = (y[q] - y[p]) / x_delta;
                let x_mean = (x[q] + x[p]) / 2.0;
                let y_mean = (y[q] + y[p]) / 2.0;
                estimates[0].push(slope * 0.25 + y_mean - slope * x_mean);
                estimates[1].push(slope * 0.75 + y_mean - slope * x_mean);
            }
        }
    }
    estimates
}

pub fn dp_med(z: &Vec<Float>, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Float {
    let lower: ArrayD<Float> = arr1(&[r_lower]).into_dyn();
    let upper: ArrayD<Float> = arr1(&[r_upper]).into_dyn();
    let n = (*z).len() as Integer;

    let mut z_clipped = Vec::new();
    for i in 0..n {
        let i: usize = i as usize;
        if z[i] >= r_lower {
            if z[i] <= r_upper {
                z_clipped.push(z[i]);
            }
        }
    }
    // let mut z_clipped: Vec<Float> = (*z).into_iter().filter(|x| (r_lower <= x)).copied().collect::<Vec<Float>>();  // clamp_numeric_float(arr1(z).into_dyn(), lower, upper).unwrap().into_raw_vec();
    z_clipped.push(r_lower);
    z_clipped.push(r_upper);
    z_clipped.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut max_noisy_score = std::f64::NEG_INFINITY;
    let mut arg_max_noisy_score: Integer = -1;

    let limit = z_clipped.len() as Integer;

    for i in 1..limit {
        let length = z_clipped[i as usize] - z_clipped[i as usize - 1 as usize];
        let log_interval_length: Float = if length <= 0.0 { std::f64::NEG_INFINITY } else { length.ln()};
        let dist_from_median = (i as Float - n as Float / 2.0).abs().ceil();

        // This term makes the score *very* sensitive to changes in epsilon
        let score = log_interval_length - (epsilon / 2.0) * dist_from_median;

        let noise_term = noise::sample_gumbel(0.0, 1.0); // gumbel1(&rng, 0.0, 1.0);
        let noisy_score: Float = score + noise_term;
        // println!("score: {} max: {} argmax: {}", noisy_score, max_noisy_score, arg_max_noisy_score);

        if noisy_score > max_noisy_score{
            max_noisy_score = noisy_score;
            arg_max_noisy_score = i;
        }
    }
    let left = z_clipped[arg_max_noisy_score as usize - 1 as usize];
    let right = z_clipped[arg_max_noisy_score as usize];
    // let rng = Rng::new();
    let median = noise::sample_uniform(left, right, enforce_constant_time).unwrap();
    return median;
}

pub fn dp_theil_sen_k_match(x: Vec<Float>, y: Vec<Float>, n: Integer, k: Integer, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Result<(Float, Float), Error> {

    // let tau = permute_range(n, k);
    // let range = (0..n).map(Integer::from).collect::<Vec<Integer>>();
    // let tau = all_permutations(range, n);
    // *Previous method before seeing Python source*
    // For sampling without replacement, shuffle this list and draw first (or last) element
    // let mut z_25 = Vec::new();
    // let mut z_75 = Vec::new();
    // let mut h_vec = (0..n).map(Integer::from).collect::<Vec<Integer>>();
    // let mut rng = thread_rng();
    // h_vec.shuffle(&mut rng);
    //
    // for i in (0..n-1).step_by(2) {
    //     // println!("theil sen: {}", i);
    //     let h = h_vec.pop().unwrap() as usize;
    //     let j = tau[h][i as usize] as usize;
    //     let l = tau[h][i as usize + 1 as usize] as usize;
    //     if x[l] - x[j] != 0.0 {
    //         let slope = (y[l] - y[j]) / (x[l] - x[j]);
    //         z_25.push(slope * (0.25 - (x[l] + x[j])/2.0) + (y[l] + y[j])/2.0);
    //         z_75.push(slope * (0.75 - (x[l] + x[j])/2.0) + (y[l] + y[j])/2.0);
    //     } else {
    //         return Err(Error::TooSteep);
    //     }
    // }

    let estimates: Vec<Vec<Float>> = compute_all_ests(x, y, n);

    let pfinal_25 = dp_med(&estimates[0], epsilon / k as Float, r_lower, r_upper, enforce_constant_time);
    let pfinal_75 = dp_med(&estimates[1], epsilon / k as Float, r_lower, r_upper, enforce_constant_time);

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
    fn permute_range_test() {
        let n = 10;
        let k = n - 1;
        let tau = permute_range(n, k);
        assert_eq!(tau.len() as Integer, k)
    }

    #[test]
    fn gumbel_test() {
        let u: Vec<Float> = (0..100000).map(|_| noise::sample_gumbel(0.0, 1.0)).collect();
        let mean = u.iter().sum::<Float>() as Float / u.len() as Float;
        println!("{}", mean);
        // Mean should be approx. mu + beta*gamma (location + scale * Euler-Mascheroni Const.)
        // Where gamma = 0.5772....
        let gamma = 0.5772;
        let tol = 0.01;
        assert!((mean - gamma).abs() < tol);
    }

    #[test]
    fn compute_estimates_test() {
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![1.0, 4.0, 9.0];
        let n = 3;
        let estimates = compute_all_ests(x, y, n);
        let expected: Vec<Vec<Float>> = vec![vec![-1.25, -2.0, -4.75], vec![0.25, 0.0, -2.25]];
        assert_eq!(expected, estimates);
    }

    #[test]
    fn dp_median_from_estimates_test() {
        let estimates: Vec<Vec<Float>> = vec![vec![-1.25, -2.0, -4.75], vec![0.25, 0.0, -2.25]];
        let true_median = 5.0;
        let median = dp_med(&estimates[0], 1e-6 as Float, 0.0, 10.0, true);
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn dp_median_test() {
        let z = vec![0.0, 2.50, 5.0, 7.50, 10.0];
        let true_median = 5.0;
        let median = dp_med(&z, 1e-6 as Float, 0.0, 10.0, true);
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn dp_theilsen_test() {
        let x: Vec<Float> = (0..10).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..10).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
        let n = x.len() as Integer;
        let k = n - 1;
        assert_eq!((2.0, 0.0), dp_theil_sen_k_match(x, y, n, k, 1.0,  0.0, 10.0, true).unwrap());
    }
}