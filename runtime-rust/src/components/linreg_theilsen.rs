use whitenoise_validator::{Float, Integer};
use permutohedron::heap_recursive;
use crate::components::linreg_error::Error;
use rand::prelude::*;
use rand::thread_rng;
use crate::utilities::{noise};
// use whitenoise_runtime::utilities::noise::sample_bit_prob;
use crate::utilities::noise::sample_bit_prob;
// use std::ptr::null;

/// Select k random values from range 1 to n
///
pub fn permute_range(n: Integer, k: Integer) -> Vec<Integer> {
    let range = (1..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, k as usize).cloned().collect();
    vec_sample.shuffle(&mut rng);
    vec_sample
}


pub fn _compute_slope(x: &Vec<Float>, y: &Vec<Float>) -> Float {
    let x_delta = x[1] - x[0];
    if x_delta != 0.0 {
        (y[1] - y[0]) / x_delta
    } else {
        Float::INFINITY
    }
}


/// Compute slope between all pairs of points where defined
///
pub fn compute_all_slope_estimates(x: &Vec<Float>, y: &Vec<Float>, n: Integer) -> Vec<Float> {
    let mut estimates: Vec<Float> = Vec::new();
    for p in 0..n as usize {
        for _q in p+1..n as usize {
            let slope = _compute_slope(&x, &y);
            if slope != Float::INFINITY {
                estimates.push(slope);
            }
        }
    }
    estimates
}

/// Marked for deletion
///
pub fn all_permutations(vec: Vec<Integer>, n: Integer) -> Vec<Vec<Integer>> {
    let mut permutations = Vec::new();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = vec.choose_multiple(&mut rng, n as usize).cloned().collect();
    heap_recursive(&mut vec_sample, |permutation| {
        permutations.push(permutation.to_vec())
    });
    permutations
}

/// My implementation of permutations of the paper
/// Leaving this here for now, though not in use.
pub fn _tau_permutations(x: Vec<Float>, y: Vec<Float>, n: Integer) -> Result<(Vec<Float>, Vec<Float>), Error> {
    // let tau = permute_range(n, k);
    let range = (0..n).collect::<Vec<Integer>>();
    let tau = all_permutations(range, n);
    // *Previous method before seeing Python source*
    // For sampling without replacement, shuffle this list and draw first (or last) element
    let mut z_25: Vec<Float> = Vec::new();
    let mut z_75: Vec<Float> = Vec::new();
    let mut h_vec = (0..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = thread_rng();
    h_vec.shuffle(&mut rng);
    for i in (0..n - 1).step_by(2) {
        // println!("theil sen: {}", i);
        let h = h_vec.pop().unwrap() as usize;
        let j = tau[h][i as usize] as usize;
        let l = tau[h][i as usize + 1 as usize] as usize;
        if x[l] - x[j] != 0.0 {
            let slope = (y[l] - y[j]) / (x[l] - x[j]);
            z_25.push(slope * (0.25 - (x[l] + x[j]) / 2.0) + (y[l] + y[j]) / 2.0);
            z_75.push(slope * (0.75 - (x[l] + x[j]) / 2.0) + (y[l] + y[j]) / 2.0);
        } else {
            return Err(Error::TooSteep);
        }
    }
    Ok((z_25, z_75))
}

pub fn dp_med(z: &Vec<Float>, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Float {
    let n = (*z).len();
    let mut z_clipped = Vec::new();
    for i in 0..n {
        if z[i] >= r_lower {
            if z[i] <= r_upper {
                z_clipped.push(z[i]);
            }
        }
    }
    z_clipped.push(r_lower);
    z_clipped.push(r_upper);
    z_clipped.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut max_noisy_score = std::f64::NEG_INFINITY;
    let mut arg_max_noisy_score: Integer = -1;

    let limit = z_clipped.len();

    for i in 1..limit {
        let length = z_clipped[i] - z_clipped[i - 1 as usize];
        let log_interval_length: Float = if length <= 0.0 { std::f64::NEG_INFINITY } else { length.ln()};
        let dist_from_median = (i as Float - (n as Float / 2.0)).abs().ceil();

        // This term makes the score *very* sensitive to changes in epsilon
        let score = log_interval_length - (epsilon / 2.0) * dist_from_median;

        let noise_term = noise::sample_gumbel(0.0, 1.0); // gumbel1(&rng, 0.0, 1.0);
        let noisy_score: Float = score + noise_term;

        if noisy_score > max_noisy_score {
            max_noisy_score = noisy_score;
            arg_max_noisy_score = i as Integer;
        }
    }
    let left = z_clipped[arg_max_noisy_score as usize - 1 as usize];
    let right = z_clipped[arg_max_noisy_score as usize];
    let median = noise::sample_uniform(left, right, enforce_constant_time).unwrap();
    return median;
}

/// Return the index of the median value
/// If even, randomly return one nearby index
pub fn median_arg(x: &Vec<Float>) -> usize {
    if x.len() % 2 == 0 {
        let n = sample_bit_prob(0.5).unwrap();
        if n == 1 {
            ((x.len() as Integer / 2) + 1) as usize
        } else {
            x.len() / (2 as usize)
        }
    } else {
        ((x.len() as Integer / 2) + 1) as usize
    }
}

/// Non-DP pairwise regression
///
pub fn pairwise_regression(x: &Vec<Float>, y: &Vec<Float>, k: Integer) -> (Float, Float) {
    let mut lines: Vec<(Float, Float)> = Vec::new();
    let indices = permute_range(k, k);
    for i in (0..indices.len()).step_by(2) {
        let slope = _compute_slope(&x[i..i+2].to_vec(), &y[i..i+2].to_vec());
        let intercept = ordinary_least_squares_intercept(&x, &y, slope);
        lines.push((slope, intercept));
    }
    lines.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let mut lines_sorted_by_intercept: Vec<(Float, Float)> = lines.clone();
    lines_sorted_by_intercept.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let median_slope_index = median_arg(&lines.iter().map(|x| x.0).collect::<Vec<Float>>());
    let median_intercept_index = median_arg(&lines_sorted_by_intercept.iter().map(|x| x.1).collect::<Vec<Float>>());

    // Get the index in lines containing the median intercept
    let mut intercept_index: usize = 0;
    for i in 0..lines.len()-1 {
        if lines[i].1 == lines_sorted_by_intercept[median_intercept_index].1 {
            intercept_index = i;
        }
    }

    let mut candidates: Vec<(Float, Float)> = Vec::new();
    if intercept_index < median_slope_index {
        candidates = lines[intercept_index..median_slope_index+1].to_vec();
    } else if median_slope_index < intercept_index {
        candidates = lines[median_slope_index..intercept_index+1].to_vec();
    } else {
        candidates = lines[median_slope_index..median_slope_index+1].to_vec();
    }
    (candidates.iter().map(|x| x.0).sum::<Float>() as Float / candidates.len() as Float,
     candidates.iter().map(|x| x.1).sum::<Float>() as Float / candidates.len() as Float)
}

/// DP pairwise regression
///
pub fn dp_pairwise_regression(x: &Vec<Float>, y: &Vec<Float>, k: Integer, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> (Float, Float) {
    let mut lines: Vec<(Float, Float)> = Vec::new();
    let indices = permute_range(k, k);
    for i in (0..indices.len()).step_by(2) {
        let slope = _compute_slope(&x[i..i+2].to_vec(), &y[i..i+2].to_vec());
        let intercept = ordinary_least_squares_intercept(&x, &y, slope);
        lines.push((slope, intercept));
    }
    lines.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let mut lines_sorted_by_intercept: Vec<(Float, Float)> = lines.clone();
    lines_sorted_by_intercept.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let median_slope_index = median_arg(&lines.iter().map(|x| x.0).collect::<Vec<Float>>());
    let median_intercept_index = median_arg(&lines_sorted_by_intercept.iter().map(|x| x.1).collect::<Vec<Float>>());

    // Get the index in lines containing the median intercept
    let mut intercept_index: usize = 0;
    for i in 0..lines.len()-1 {
        if lines[i].1 == lines_sorted_by_intercept[median_intercept_index].1 {
            intercept_index = i;
        }
    }

    let mut candidates: Vec<(Float, Float)> = Vec::new();
    if intercept_index < median_slope_index {
        candidates = lines[intercept_index..median_slope_index+1].to_vec();
    } else if median_slope_index < intercept_index {
        candidates = lines[median_slope_index..intercept_index+1].to_vec();
    } else {
        candidates = lines[median_slope_index..median_slope_index+1].to_vec();
    }
    let median_slope = dp_med(&candidates.iter().map(|x| x.0).collect::<Vec<Float>>(), epsilon, r_lower, r_upper, enforce_constant_time);
    let median_intercept = dp_med(&candidates.iter().map(|x| x.1).collect::<Vec<Float>>(), epsilon, r_lower, r_upper, enforce_constant_time);

    (median_slope, median_intercept)
}

/// Randomly select k points from x and y (k < n) and then perform DP-TheilSen.
/// Useful for larger datasets where calculating on n^2 points is less than ideal.
pub fn dp_theil_sen_k_match(x: &Vec<Float>, y: &Vec<Float>, n: Integer, k: Integer, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Result<(Float, Float), Error> {
    let indices: Vec<usize> = permute_range(n, k).iter().map(|x| *x as usize).collect::<Vec<usize>>();
    let mut x_kmatch: Vec<Float> = Vec::new();
    let mut y_kmatch: Vec<Float> = Vec::new();
    let scaled_epsilon = epsilon / (k as Float);
    for i in indices {
        // let index: usize = indices[i] as usize;
        x_kmatch.push(x[i]);
        y_kmatch.push(y[i]);
    }
    dp_theil_sen(&x_kmatch, &y_kmatch, k, scaled_epsilon, r_lower, r_upper, enforce_constant_time)
}

/// DP-TheilSen over all n points in data
///
pub fn dp_theil_sen(x: &Vec<Float>, y: &Vec<Float>, n: Integer, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Result<(Float, Float), Error> {
    let estimates: Vec<Float> = compute_all_slope_estimates(x, y, n);

    let slope = dp_med(&estimates, epsilon, r_lower, r_upper, enforce_constant_time);

    let mut diffs: Vec<Float> = Vec::new();
    for i in 0..x.len() {
        diffs.push(y[i] - slope*x[i]);
    }

    let mut x_sort = x.clone();
    x_sort.sort_by(|a, b| a.partial_cmp(b).unwrap());
    // let max_value = (x_sort[x_sort.len()-1] / 10.0).ceil();
    let intercept = ordinary_least_squares_intercept(&x, &y, slope);

    Ok((slope, intercept))

}

pub fn median(x: &Vec<Float>) -> Float {
    let mut tmp: Vec<Float> = x.clone();
    tmp.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = tmp.len() / 2;
    if tmp.len() % 2 == 0 {
        (tmp[mid-1] + tmp[mid]) / 2.0
    } else {
        tmp[mid]
    }
}

/// Non-DP Estimate for y intercept,
/// using x_mean and y_mean
pub fn ordinary_least_squares_intercept(x: &Vec<Float>, y: &Vec<Float>, slope: Float) -> Float {
    // let intercept_estimate = dp_med(&y_clipped, epsilon, y_clipped[0], y_clipped[y_clipped.len()-1], enforce_constant_time);
    let y_mean = y.iter().sum::<Float>() as Float / x.len() as Float;
    let x_mean = x.iter().sum::<Float>() as Float / x.len() as Float;
    let intercept_estimate = y_mean  - slope * x_mean;
    intercept_estimate
}

/// Non-DP implementation of Theil-Sen to test DP version against
///
pub fn theil_sen(x: &Vec<Float>, y: &Vec<Float>, n: Integer) -> (Float, Float) {

    // Slope m is median of slope calculated between all pairs of
    // non-identical points
    let slope_estimates: Vec<Float> = compute_all_slope_estimates(x, y, n);
    let slope = median(&slope_estimates);

    // Intercept is median of set of points y_i - m * x_i
    let mut diffs: Vec<Float> = Vec::new();
    for i in 0..x.len() as Integer {
        diffs.push(y[i as usize] - slope*x[i as usize]);
    }
    let intercept = median(&diffs);

    return (slope, intercept)

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
        let estimates = compute_all_slope_estimates(&x, &y, n);
        let expected: Vec<Float> = vec![3.0, 4.0, 5.0];
        assert_eq!(expected, estimates);
    }

    #[test]
    fn theilsen_test() {
        // Ensure non-DP version gives y = 2x for this data
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
        let n = x.len() as Integer;
        let theilsen_estimate = theil_sen(&x, &y, n);
        assert_eq!((2.0, 0.0), theilsen_estimate);
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
    fn intercept_estimation_test() {
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
        let intercept = ordinary_least_squares_intercept(&x, &y, 2.0);
        println!("Estimated Intercept: {}", intercept);
        assert!(intercept.abs() <= 5.0);
    }

    #[test]
    fn dp_theilsen_test() {
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let x_mut = x.clone();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).map(|x| x + noise::sample_gaussian(0.0, 0.1, true)).collect::<Vec<Float>>();
        let y_mut = y.clone();
        let n = x.len() as Integer;
        let k = n - 1;
        let epsilon = 1000000.0;
        let (slope, intercept) = theil_sen(&x, &y, 1000);
        let (dp_slope, dp_intercept) = dp_theil_sen_k_match(&x_mut, &y_mut, n, k, epsilon,  0.0, 2.0, true).unwrap();

        // println!("Theil-Sen Slope Estimate: {}, {}", slope, intercept);
        // println!("DP Theil-Sen Slope Estimate: {}, {}", dp_slope, dp_intercept);

        assert!((dp_slope - slope).abs() <= 1.0 / epsilon);
        assert!((dp_intercept - intercept).abs() <= (n as Float) * (1.0 / epsilon));
    }

    #[test]
    fn dp_theilsen_epsilon_test() {
        let mut results: Vec<(Float, Float)> = Vec::new();
        for epsilon in vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 1e4 as Float, 1e5 as Float, 1e6 as Float] {
            println!("Epsilon: {}", epsilon);
            let n = 100;
            let x: Vec<Float> = (0..n).map(Float::from).collect::<Vec<Float>>();
            let y: Vec<Float> = (0..n).map(|x| 2 * x).map(Float::from).map(|x| x + noise::sample_gaussian(0.0, 0.0001, true)).collect::<Vec<Float>>();
            let k = n - 1;
            let (slope, intercept) = theil_sen(&x, &y, 100);
            let (dp_slope, dp_intercept) = dp_theil_sen_k_match(&x, &y, n as Integer, k as Integer, epsilon,  0.0, 2.0, true).unwrap();
            results.push(((dp_slope-slope).abs(), (dp_intercept-intercept).abs()));
            println!("Theil-Sen Estimate Difference: {}, {}", (dp_slope-slope).abs(), (dp_intercept-intercept).abs());
            assert!((dp_slope - slope).abs() <= 1.0 / epsilon);
            assert!((dp_intercept - intercept).abs() <= (n as Float) * (1.0 / epsilon));
        }
    }

    #[test]
    fn pairwise_test() {
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).map(|x| x + noise::sample_gaussian(0.0, 0.1, true)).collect::<Vec<Float>>();
        let n = x.len() as Integer;
        let (slope, intercept) = pairwise_regression(&x, &y, n);
        println!("Pairwise Estimate: {} {}", slope, intercept);

        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).map(|x| x + noise::sample_gaussian(0.0, 0.1, true)).collect::<Vec<Float>>();
        let n = x.len() as Integer;

        let (ts_slope, ts_intercept) = theil_sen(&x, &y, n);
        println!("Theil-Sen Estimate: {} {}", ts_slope, ts_intercept);

        assert!((ts_slope - slope).abs() < 0.01);
        // Intercept estimates tend to differ more sharply
        assert!((ts_intercept - intercept).abs() < 10.0);
    }

    #[test]
    fn dp_pairwise_test() {
        for epsilon in vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 1e4 as Float, 1e5 as Float, 1e6 as Float] {
            println!("Epsilon: {}", epsilon);
            let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
            let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).map(|x| x + noise::sample_gaussian(0.0, 0.1, true)).collect::<Vec<Float>>();
            let n = x.len() as Integer;
            let (slope, intercept) = dp_pairwise_regression(&x, &y, n, epsilon, 1.0, 3.0, true);
            println!("DP Pairwise Estimate: {} {}", slope, intercept);

            let x_ts: Vec<Float> = x.clone();
            let y_ts: Vec<Float> = y.clone();
            let n_ts = x_ts.len() as Integer;

            let (ts_slope, ts_intercept) = theil_sen(&x_ts, &y_ts, n_ts);
            println!("Theil-Sen Estimate: {} {}", ts_slope, ts_intercept);

            assert!((ts_slope - slope).abs() < n as Float);
            // Intercept estimates tend to differ more sharply
            assert!((ts_intercept - intercept).abs() < n as Float);
        }
    }
}