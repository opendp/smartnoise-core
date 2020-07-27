/// Beginnings of DP Linear Regression
/// Borrowing heavily from the crate 'linreg'

/// use num_traits::float::{Float, FloatCore};

use core::iter::Iterator;
use crate::utilities::mechanisms::laplace_mechanism;
use whitenoise_validator::Float;

use crate::components::linreg_error::Error;

/// Calculates "NoisyStat", which adds Laplace noise to the OLS sufficient statistics
///
fn _noisy_stats_linreg(data_x: Vec<Float>, data_y: Vec<Float>, epsilon: Float, enforce_constant_time: bool) -> Result<(Float, Float), Error>
{
    let data_size: Float = data_x.len() as Float;
    assert_eq!(data_size, data_y.len() as Float);

    let mean = |data: &Vec<Float>| data.iter().sum::<Float>() / data.len() as Float;

    let x_mean = mean(&data_x);
    let y_mean = mean(&data_y);

    let delta: Float = 1.0 - 1.0 / data_size;

    let laplace_1: Float = laplace_mechanism(epsilon, 3.0 * delta, enforce_constant_time).unwrap();
    let laplace_2: Float = laplace_mechanism(epsilon, 3.0 * delta, enforce_constant_time).unwrap();

    let xxm2: f64 = data_x.iter()
        .map(|x| (x - x_mean).powi(2))
        .sum();

    let xmym2: f64 = data_x.iter().zip(data_y)
        .map(|(x, y)| (x - x_mean) * (y - y_mean))
        .sum();

    let slope = (xmym2 + laplace_1) / (xxm2 + laplace_2);

    let delta_2 = (1.0 / data_size) * (1.0 + slope.abs());

    let laplace_3: Float = laplace_mechanism(epsilon, 3.0 * delta_2, false).unwrap();

    let intercept = y_mean - slope * x_mean + laplace_3;


    // we check for divide-by-zero after the fact
    if slope.is_nan() {
        return Err(Error::TooSteep);
    }

    Ok((slope, intercept))
}

/// Calculate noisy linreg, then return "quartiles" consistent with implementation from paper
///
pub fn noisy_stats(data_x: Vec<Float>, data_y: Vec<Float>, epsilon: Float, enforce_constant_time: bool) -> Result<(Float, Float), Error>
{
    let (slope, intercept) = _noisy_stats_linreg(data_x, data_y, epsilon, enforce_constant_time).unwrap();
    Ok((0.25 * slope + intercept, 0.75 * slope + intercept))
}

#[cfg(test)]
mod tests {
    use std::vec::Vec;

    use super::*;

    #[test]
    #[allow(unused_must_use)]
    #[should_panic]
    fn unequal_x_and_y_test() {
        let x: Vec<Float> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y: Vec<Float> = vec![2.0, 4.0, 5.0, 6.0, 7.0, 9.0, 10.0];
        let epsilon = 0.1;
        let enforce_constant_time = false;
        _noisy_stats_linreg(x, y, epsilon, enforce_constant_time);
    }

    #[test]
    fn noisy_stats_completes_test() {
        let x: Vec<Float> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y: Vec<Float> = vec![2.0, 4.0, 5.0, 6.0, 7.0];
        let epsilon = 0.1;
        let enforce_constant_time = false;
        let result = _noisy_stats_linreg(x, y, epsilon, enforce_constant_time);

        // This is, admittedly, not the greatest test, but it does ensure that noisy_stats
        // is returning values without panicking.
        assert!(!result.is_err());
    }

    #[test]
    fn test_large_epsilon_test() {
        for epsilon in [1.0, 10.0, 100.0, 10000000.0].iter() {
            // Create data which describes y = 2x
            let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
            let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
            let true_slope = 2.0;
            let true_intercept = 0.0;

            let enforce_constant_time = false;
            let (slope, intercept) = _noisy_stats_linreg(x, y, *epsilon, enforce_constant_time).unwrap();
            let slope_diff = (slope - true_slope).abs();
            let intercept_diff = (intercept - true_intercept).abs();

            // println!("{} {} {}", slope_diff, intercept_diff, 1.0/epsilon);

            assert!(slope_diff < 1.0 / epsilon);
            assert!(intercept_diff < 1.0 / epsilon);
        }
    }

    #[test]
    fn quartiles_test() {
        let x: Vec<Float> = (0..1000).map(Float::from).collect::<Vec<Float>>();
        let y: Vec<Float> = (0..1000).map(|x| 2 * x).map(Float::from).collect::<Vec<Float>>();
        let true_slope = 2.0;
        let true_intercept = 0.0;
        let base_p25 = 0.25 * true_slope + true_intercept;
        let base_p75 = 0.75 * true_slope + true_intercept;

        let epsilon = 10.0;
        let enforce_constant_time = false;

        let (p_25, p_75) = noisy_stats(x, y, epsilon, enforce_constant_time).unwrap();

        // println!("{} {} {}", base_p75, p_75, 1.0/epsilon);

        assert!((base_p25 - p_25).abs() < 1.0/epsilon);
        assert!((base_p75 - p_75).abs() < 1.0/epsilon);
    }
}
