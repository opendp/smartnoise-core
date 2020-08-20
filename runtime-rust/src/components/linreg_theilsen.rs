use whitenoise_validator::{proto, Float, Integer};
use whitenoise_validator::errors::*;
use rand::prelude::*;
use crate::utilities::{noise};
use whitenoise_validator::base::{ReleaseNode};
use crate::components::Evaluable;
use whitenoise_validator::utilities::take_argument;
use ndarray::{Array, ArrayD, Axis, IxDyn, stack};
use crate::NodeArguments;


impl Evaluable for proto::TheilSen {

    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let x = take_argument(&mut arguments, "data_x")?.array()?.float()?;
        let y = take_argument(&mut arguments, "data_y")?.array()?.float()?;

        match self.implementation.to_lowercase().as_str() {
            "theil-sen" => {
                let (slopes, intercepts) = compute_all_estimates(&x, &y);
                Ok(ReleaseNode::new(stack![Axis(1), slopes, intercepts].into()))
            },
            "theil-sen-k-match" => {
                let (slopes, intercepts) = dp_theil_sen_k_match(&x, &y, take_argument(&mut arguments, "k")?.array()?.first_int()?).unwrap();
                Ok(ReleaseNode::new(stack![Axis(1), slopes, intercepts].into()))
            }
            _ => {
                panic!("Invalid implementation: {}", self.implementation.to_lowercase().as_str());
            }
        }
    }
}


/// Select k random values from range 1 to n
///
pub fn permute_range(n: Integer, k: Integer) -> Vec<Integer> {
    let range = (1..n).map(Integer::from).collect::<Vec<Integer>>();
    let mut rng = rand::thread_rng();
    let mut vec_sample: Vec<Integer> = range.choose_multiple(&mut rng, k as usize).cloned().collect();
    vec_sample.shuffle(&mut rng);
    vec_sample
}

/// Calculate slope between two points
///
pub fn _compute_slope(x: &Vec<Float>, y: &Vec<Float>) -> Float {
    let x_delta = x[1] - x[0];
    if x_delta != 0.0 {
        (y[1] - y[0]) / x_delta
    } else {
        Float::INFINITY
    }
}

/// Non-DP Estimate for y intercept,
/// using x_mean and y_mean
pub fn _compute_intercept(x: &Vec<Float>, y: &Vec<Float>, slope: Float) -> Float {
    // let intercept_estimate = dp_med(&y_clipped, epsilon, y_clipped[0], y_clipped[y_clipped.len()-1], enforce_constant_time);
    let y_mean = y.iter().sum::<Float>() as Float / x.len() as Float;
    let x_mean = x.iter().sum::<Float>() as Float / x.len() as Float;
    let intercept_estimate = y_mean  - slope * x_mean;
    intercept_estimate
}

/// Compute slope between all pairs of points where defined
///
pub fn compute_all_estimates(x: &ArrayD<Float>, y: &ArrayD<Float>) -> (ArrayD<Float>, ArrayD<Float>) {
    let n = x.len();
    let mut slopes: Vec<Float> = Vec::new(); // ArrayD::<Float>::zeros(IxDyn(&[n])) = ();
    let mut intercepts: Vec<Float> = Vec::new();

    assert_eq!(x.len(), y.len());

    for p in 0..n as usize {
        for q in p+1..n as usize {
            let mut x_pair: Vec<Float> = Vec::new();
            x_pair.push(x[p]);
            x_pair.push(x[q]);
            let mut y_pair: Vec<Float> = Vec::new();
            y_pair.push(y[p]);
            y_pair.push(y[q]);
            let slope = _compute_slope(&x_pair, &y_pair);
            if slope != Float::INFINITY {
                slopes.push(slope);
                intercepts.push(_compute_intercept(&x_pair, &y_pair, slope));
            }
        }
    }
    (Array::from(slopes).into_dyn(), Array::from(intercepts).into_dyn())
}

pub fn dp_med(z: &ArrayD<Float>, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Float {
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

/// DP-TheilSen over all n points in data
///
pub fn dp_theil_sen(x: &ArrayD<Float>, y: &ArrayD<Float>, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Result<(Float, Float)> {
    let (slopes, intercepts) = compute_all_estimates(x, y);

    let slope = dp_med(&slopes, epsilon, r_lower, r_upper, enforce_constant_time);
    let intercept = dp_med(&intercepts, epsilon, r_lower, r_upper, enforce_constant_time);

    Ok((slope, intercept))
}

/// Implementation from paper
/// Separate data into two bins, match members of each bin to form pairs
/// Note: k is number of trials here
pub fn dp_theil_sen_k_match(x: &ArrayD<Float>, y: &ArrayD<Float>, k: Integer) -> Result<(ArrayD<Float>, ArrayD<Float>)> {
    let mut slopes: Vec<Float> = Vec::new();
    let mut intercepts: Vec<Float> = Vec::new();

    let n = x.len();
    assert_eq!(x.len(), y.len());

    for _iteration in 0..k {
        let mut shuffled: Vec<(Float, Float)> = x.iter().map(|a| (*a)).zip(y.iter().map(|a| (*a))).collect();
        let mut rng = rand::thread_rng();
        shuffled.shuffle(&mut rng);

        // For n odd, the last data point in "shuffled" will be ignored
        let midpoint = (n as Float/2.0).floor() as usize;
        let bin_a: Vec<(Float, Float)> = shuffled[0..midpoint].to_vec();
        let bin_b: Vec<(Float, Float)> = shuffled[midpoint..midpoint*(2 as usize)].to_vec();
        assert_eq!(bin_a.len(), bin_b.len());

        for i in 0..bin_a.len() {
            let x: Vec<Float> = vec![bin_a[i].0, bin_b[i].0];
            let y: Vec<Float> = vec![bin_a[i].1, bin_b[i].1];
            let slope = _compute_slope(&x, &y);
            if slope != Float::INFINITY {
                let intercept = _compute_intercept(&x, &y, slope);
                slopes.push(slope);
                intercepts.push(intercept);
            }
        }
    }

    // Try to do this as one call to multidimensional median
    // let slope = dp_med(&slopes, epsilon, r_lower, r_upper, enforce_constant_time);
    // let intercept = dp_med(&intercepts, epsilon, r_lower, r_upper, enforce_constant_time);

    Ok((Array::from(slopes).into_dyn(), Array::from(intercepts).into_dyn()))

}

/// Randomly select k points from x and y (k < n) and then perform DP-TheilSen.
/// Useful for larger datasets where calculating on n^2 points is less than ideal.
pub fn dp_theil_sen_k_subset(x: &ArrayD<Float>, y: &ArrayD<Float>, n: Integer, k: Integer, epsilon: Float, r_lower: Float, r_upper: Float, enforce_constant_time: bool) -> Result<(Float, Float)> {
    let indices: Vec<usize> = permute_range(n, k).iter().map(|x| *x as usize).collect::<Vec<usize>>();
    let mut x_kmatch = ArrayD::<f64>::zeros(IxDyn(&[n as usize, 1, 1]));
    let mut y_kmatch = ArrayD::<f64>::zeros(IxDyn(&[n as usize, 1, 1]));
    let scaled_epsilon = epsilon / (k as Float);
    let mut j = 0;
    for i in indices {
        // let index: usize = indices[i] as usize;
        x_kmatch[j] = x[i];
        y_kmatch[j] = y[i];
        j+=1;
    }
    dp_theil_sen(&x_kmatch, &y_kmatch, scaled_epsilon, r_lower, r_upper, enforce_constant_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

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

    /// Non-DP implementation of Theil-Sen to test DP version against
    ///
    pub fn theil_sen(x: &ArrayD<Float>, y: &ArrayD<Float>) -> (Float, Float) {

        // Slope m is median of slope calculated between all pairs of
        // non-identical points
        let (slopes, intercepts) = compute_all_estimates(x, y);
        let slope = median(&slopes.into_raw_vec());
        let intercept = median(&intercepts.into_raw_vec());

        return (slope, intercept)

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
        let tol = 0.1;
        assert!((mean - gamma).abs() < tol);
    }

    #[test]
    fn compute_estimates_test() {
        let x = Array::range(0., 11., 1.).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let y = Array::range(0., 11., 1.).mapv(|a: f64| 2.0 * a).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let (slopes, intercepts) = compute_all_estimates(&x, &y);

        let n = x.len() as Integer;
        assert_eq!(slopes.len() as Integer, n * (n-1) / 2);
        assert_eq!(intercepts.len() as Integer, n * (n-1) / 2);

    }

    #[test]
    fn theilsen_test() {
        // Ensure non-DP version gives y = 2x for this data
        let x = Array::range(0., 10., 1.).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let y = Array::range(0., 10., 1.).mapv(|a: f64| 2.0 * a).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let (slope, intercept) = theil_sen(&x, &y);
        assert!((2.0 - slope).abs() <= 0.1);
        assert!((0.0 - intercept).abs() <= 0.1);
    }

    #[test]
    fn dp_median_from_estimates_test() {
        let estimates = array![-1.25, -2.0, -4.75].into_dyn();
        let true_median = 5.0;
        let median = dp_med(&estimates, 1e-6 as Float, 0.0, 10.0, true);
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn dp_median_test() {
        let z =  array![0.0, 2.50, 5.0, 7.50, 10.0].into_dyn();
        let true_median = 5.0;
        let median = dp_med(&z, 1e-6 as Float, 0.0, 10.0, true);
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn intercept_estimation_test() {
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
        let intercept = _compute_intercept(&x, &y, 2.0);
        println!("Estimated Intercept: {}", intercept);
        assert!(intercept.abs() <= 5.0);
    }

    #[test]
    fn dp_theilsen_test() {
        let x = Array::range(0., 10., 1.).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let x_mut = x.clone();
        let y = Array::range(0., 10., 1.).mapv(|a: f64| 2.0 * a).mapv(|a: f64| a + noise::sample_gaussian(0.0, 0.1, true)).into_dyn();
        let y_mut = y.clone();
        let n = x.len() as Integer;
        let k = n - 1;
        let epsilon = 1.0;
        let (slope, intercept) = theil_sen(&x, &y);
        let (dp_slope_candidates, dp_intercept_candidates) = dp_theil_sen_k_match(&x_mut, &y_mut, k).unwrap();

        assert_eq!(dp_slope_candidates.len() as Integer, k * (n / 2));
        assert_eq!(dp_intercept_candidates.len() as Integer, k * (n / 2));

        let dp_slope = dp_med(&dp_slope_candidates, epsilon, 0.0, 2.0, true);
        let dp_intercept = dp_med(&dp_intercept_candidates, epsilon, 0.0, 2.0, true);
        // println!("Theil-Sen Slope Estimate: {}, {}", slope, intercept);
        // println!("DP Theil-Sen Slope Estimate: {}, {}", dp_slope, dp_intercept);
        println!("Theil-Sen Estimate Difference: {}, {}", (dp_slope-slope).abs(), (dp_intercept-intercept).abs());

        assert!((dp_slope - slope).abs() <= (n.pow(4) as Float) / epsilon);
        assert!((dp_intercept - intercept).abs() <= (n.pow(4)  as Float) * (1.0 / epsilon));

    }
}
