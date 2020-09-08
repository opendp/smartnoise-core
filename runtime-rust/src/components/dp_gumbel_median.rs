use std::cmp::Ordering;

use rand::seq::SliceRandom;

use whitenoise_validator::{Float, Integer, proto};
use whitenoise_validator::base::ReleaseNode;
use whitenoise_validator::errors::*;
use whitenoise_validator::utilities::privacy::{get_epsilon, spread_privacy_usage};
use whitenoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;
use crate::utilities::{noise};

impl Evaluable for proto::DpGumbelMedian {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?.vec_float(None)?;
        let epsilon = get_epsilon(&spread_privacy_usage(&self.privacy_usage, 1)?[0])?;

        let lower = take_argument(&mut arguments, "lower")?.array()?.first_float()?;
        let upper = take_argument(&mut arguments, "upper")?.array()?.first_float()?;

        let enforce_constant_time = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?
            .protect_elapsed_time;

        let median = dp_gumbel_median(data, epsilon, lower, upper, enforce_constant_time)?;

        Ok(ReleaseNode::new(median.into()))
    }
}


/// Select k random values from range 1 to n
///
pub fn permute_range(n: Integer, k: Integer) -> Vec<Integer> {
    let range = (1..n as Integer).collect::<Vec<Integer>>();
    let mut rng = rand::thread_rng();
    range.choose_multiple(&mut rng, k as usize).cloned().collect()
}

/// This follows closely the DP Median implementation from the paper, including notation
///
fn dp_gumbel_median(
    z: Vec<Float>, epsilon: Float,
    lower: Float, upper: Float,
    enforce_constant_time: bool,
) -> Result<Float> {
    let mut z_clipped = z.into_iter()
        .filter(|v| &lower <= v && v <= &upper)
        .chain(vec![lower, upper])
        .collect::<Vec<_>>();
    z_clipped.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let mut max_noisy_score = f64::NEG_INFINITY;
    let mut arg_max_noisy_score: usize = 0;

    let limit = z_clipped.len();
    if limit == 0 { return Err("empty candidate set".into()) }

    for i in 1..limit {
        let length = z_clipped[i] - z_clipped[i - 1];
        let dist_from_median = (i as Float - (z_clipped.len() as Float / 2.0)).abs().ceil();

        // This term makes the score *very* sensitive to changes in epsilon
        let score = length.ln() - (epsilon / 2.0) * dist_from_median;

        let noise_term = noise::sample_gumbel(0.0, 1.0); // gumbel1(&rng, 0.0, 1.0);
        let noisy_score: Float = score + noise_term;

        if noisy_score > max_noisy_score {
            max_noisy_score = noisy_score;
            arg_max_noisy_score = i;
        }
    }

    let left = z_clipped[arg_max_noisy_score - 1];
    let right = z_clipped[arg_max_noisy_score];
    let median = noise::sample_uniform(left, right, enforce_constant_time)?;
    Ok(median)
}


#[cfg(test)]
pub mod test {
    use ndarray::{array, ArrayD};

    use whitenoise_validator::{Float, Integer};
    use whitenoise_validator::errors::*;

    use crate::components::dp_gumbel_median::{dp_gumbel_median, permute_range};
    use crate::components::linreg_theilsen::{theil_sen_transform, theil_sen_transform_k_match};
    use crate::components::linreg_theilsen::tests::{public_theil_sen, test_dataset};
    use crate::utilities::noise;

    /// Randomly select k points from x and y (k < n) and then perform DP-TheilSen.
        /// Useful for larger datasets where calculating on n^2 points is less than ideal.
    pub fn dp_theil_sen_k_subset(
        x: &ArrayD<Float>, y: &ArrayD<Float>,
        n: Integer, k: Integer, epsilon: Float,
        lower: Float, upper: Float,
        enforce_constant_time: bool,
    ) -> Result<(Float, Float)> {
        let indices: Vec<usize> = permute_range(n, k).iter()
            .map(|x| *x as usize).collect::<Vec<usize>>();

        let x_kmatch = x.select(ndarray::Axis(0), &indices)
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();
        let y_kmatch = y.select(ndarray::Axis(0), &indices)
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();

        let scaled_epsilon = epsilon / (k as Float);
        dp_theil_sen(&x_kmatch, &y_kmatch, scaled_epsilon, lower, upper, enforce_constant_time)
    }

    /// DP-TheilSen over all n points in data
    pub fn dp_theil_sen(
        x: &Vec<Float>, y: &Vec<Float>,
        epsilon: Float,
        r_lower: Float, r_upper: Float,
        enforce_constant_time: bool,
    ) -> Result<(Float, Float)> {
        let (slopes, intercepts) = theil_sen_transform(x, y)?;

        let slope = dp_gumbel_median(slopes, epsilon, r_lower, r_upper, enforce_constant_time)?;
        let intercept = dp_gumbel_median(intercepts, epsilon, r_lower, r_upper, enforce_constant_time)?;

        Ok((slope, intercept))
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
    fn dp_median_from_estimates_test() {
        let estimates = vec![-1.25, -2.0, -4.75];
        let true_median = 5.0;
        let median = dp_gumbel_median(
            estimates, 1e-6 as Float,
            0.0, 10.0, true).unwrap();
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn dp_median_column_test() {
        let z = vec![0.0, 2.50, 5.0, 7.50, 10.0];
        let true_median = 5.0;
        let median = dp_gumbel_median(z, 1e-6 as Float, 0.0, 10.0, true).unwrap();
        assert!((true_median - median).abs() / true_median < 1.0);
    }

    #[test]
    fn dp_theilsen_test() {
        let n = 10;
        let (x, y) = test_dataset(n);

        let k = n - 1;
        let epsilon = 1.0;

        let (slope, intercept) = public_theil_sen(&x, &y);
        let (dp_slope_candidates, dp_intercept_candidates) = theil_sen_transform_k_match(&x, &y, k).unwrap();

        assert_eq!(dp_slope_candidates.len() as Integer, k * (n / 2));
        assert_eq!(dp_intercept_candidates.len() as Integer, k * (n / 2));

        let dp_slope = dp_gumbel_median(dp_slope_candidates, epsilon, 0.0, 2.0, true).unwrap();
        let dp_intercept = dp_gumbel_median(dp_intercept_candidates, epsilon, 0.0, 2.0, true).unwrap();

        // println!("Theil-Sen Slope Estimate: {}, {}", slope, intercept);
        // println!("DP Theil-Sen Slope Estimate: {}, {}", dp_slope, dp_intercept);
        println!("Theil-Sen Estimate Difference: {}, {}", (dp_slope - slope).abs(), (dp_intercept - intercept.clone()).abs());

        assert!((dp_slope - slope).abs() <= (n.pow(4) as Float) / epsilon);
        assert!((dp_intercept - intercept).abs() <= (n.pow(4) as Float) * (1.0 / epsilon));
    }
}
