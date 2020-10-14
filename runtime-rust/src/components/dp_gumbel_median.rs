use std::cmp::Ordering;

use smartnoise_validator::{Float, proto};
use smartnoise_validator::base::ReleaseNode;
use smartnoise_validator::errors::*;
use smartnoise_validator::utilities::privacy::get_epsilon;
use smartnoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;
use crate::utilities::noise;

impl Evaluable for proto::DpGumbelMedian {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?.vec_float(None)?;

        if self.privacy_usage.len() != 1 {
            return Err(Error::from("DPGumbelMedian is not vectorized, only one privacy parameter may be passed"))
        }
        let epsilon = get_epsilon(&self.privacy_usage[0])?;

        let lower = take_argument(&mut arguments, "lower")?.array()?.first_float()?;
        let upper = take_argument(&mut arguments, "upper")?.array()?.first_float()?;

        let enforce_constant_time = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?
            .protect_elapsed_time;

        let median = dp_gumbel_median(data, epsilon, lower, upper, enforce_constant_time)?;

        Ok(ReleaseNode {
            value: median.into(),
            privacy_usages: Some(self.privacy_usage.clone()),
            public: true,
        })
    }
}

/// This follows closely the DP Median implementation from the paper, including notation
///
fn dp_gumbel_median(
    z: Vec<Float>, epsilon: Float,
    lower: Float, upper: Float,
    enforce_constant_time: bool,
) -> Result<Float> {
    // ensure there is always a score that is not negative infinity
    if lower >= upper {
        return Err(Error::from("lower must be less than upper"))
    }

    let mut z_clipped = z.into_iter()
        .filter(|v| !v.is_nan())
        .map(|v| if v < lower { lower } else if v > upper { upper } else { v })
        .chain(vec![lower, upper])
        .collect::<Vec<_>>();
    z_clipped.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let mut max_noisy_score = f64::NEG_INFINITY;
    let mut arg_max_noisy_score: usize = 0;

    for i in 1..z_clipped.len() {
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
    use ndarray::ArrayD;

    use smartnoise_validator::{Float, Integer};
    use smartnoise_validator::errors::*;
    use smartnoise_validator::proto::privacy_definition::Neighboring;

    use crate::components::dp_gumbel_median::{dp_gumbel_median};
    use crate::components::theil_sen::{theil_sen_transform, theil_sen_transform_k_match};
    use crate::components::theil_sen::tests::{public_theil_sen, test_dataset};
    use crate::components::resize::create_sampling_indices;
    use crate::utilities::noise;

    /// Randomly select k points from x and y (k < n) and then perform DP-TheilSen.
            /// Useful for larger datasets where calculating on n^2 points is less than ideal.
    pub fn dp_theil_sen_k_subset(
        x: &ArrayD<Float>, y: &ArrayD<Float>,
        n: Integer, k: Integer, epsilon: Float,
        lower: Float, upper: Float,
        enforce_constant_time: bool,
    ) -> Result<(Float, Float)> {
        let indices: Vec<usize> = create_sampling_indices(k, n, enforce_constant_time)?;

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
        let (slopes, intercepts) = theil_sen_transform(x, y, Neighboring::AddRemove)?;

        let slope = dp_gumbel_median(slopes, epsilon, r_lower, r_upper, enforce_constant_time)?;
        let intercept = dp_gumbel_median(intercepts, epsilon, r_lower, r_upper, enforce_constant_time)?;

        Ok((slope, intercept))
    }

    #[test]
    fn create_sampling_indices_test() {
        let n = 10;
        let k = n - 1;
        let tau = create_sampling_indices(k, n, false).unwrap();
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
        let (dp_slope_candidates, dp_intercept_candidates) = theil_sen_transform_k_match(&x, &y, k, Neighboring::AddRemove, false).unwrap();

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
