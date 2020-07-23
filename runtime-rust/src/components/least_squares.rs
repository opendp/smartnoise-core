/// Beginnings of DP Linear Regression
/// Borrowing heavily from the Rust builting 'linreg'

/// use num_traits::float::{Float, FloatCore};

use core::iter::Iterator;
use displaydoc::Display;
use crate::utilities::mechanisms::laplace_mechanism;
use whitenoise_validator::Float;

/// The kinds of errors that can occur when calculating a linear regression.
#[derive(Copy, Clone, Display, Debug, PartialEq)]
pub enum Error {
    /// The slope is too steep to represent, approaching infinity.
    TooSteep,
    /// Failed to calculate mean.
    ///
    /// This means the input was empty or had too many elements.
    Mean,
    /// Lengths of the inputs are different.
    InputLenDif,
    /// Can't compute linear regression of zero elements
    NoElements,
}

/// Calculates "NoisyStat", which adds Laplace noise to the OLS sufficient statistics
///
pub fn noisy_stats<I>(xys: I, x_mean: Float, y_mean: Float, epsilon: Float) -> Result<(Float, Float), Error>
    where
        I: Iterator<Item=(Float, Float)>,
{

    let data_size_hint: (usize, Option<usize>) = xys.size_hint();

    let data_size: Float = data_size_hint.0 as Float;

    let delta: Float = 1.0 - 1.0 / data_size;

    let laplace_1: Float = laplace_mechanism(epsilon, 3.0*delta, false).unwrap();
    let laplace_2: Float = laplace_mechanism(epsilon, 3.0*delta, false).unwrap();

    // SUM (x-mean(x))^2
    let mut xxm2 = 0.0;

    // SUM (x-mean(x)) (y-mean(y))
    let mut xmym2 = 0.0;

    for (x, y) in xys {
        xxm2 = xxm2 + (x - x_mean) * (x - x_mean);
        xmym2 = xmym2 + (x - x_mean) * (y - y_mean);
    }

    let slope = (xmym2 + laplace_1) / (xxm2 + laplace_2);

    let delta_2 = (1.0 / data_size) * (1.0 + slope.abs());

    let laplace_3: Float = laplace_mechanism(epsilon, 3.0*delta_2, false).unwrap();

    let intercept = y_mean - slope * x_mean + laplace_3;

    let p_25 = 0.25 * slope + intercept;
    let p_75 = 0.75 * slope + intercept;

    // we check for divide-by-zero after the fact
    if slope.is_nan() {
        return Err(Error::TooSteep);
    }

    Ok((p_25, p_75))
}


#[cfg(test)]
mod tests {
    use std::vec::Vec;

    use super::*;

    #[test]
    fn noisy_stats_test() {
        let tuples: Vec<(Float, Float)> =
            vec![(1.0, 2.0), (2.0, 4.0), (3.0, 5.0), (4.0, 4.0), (5.0, 5.0)];
        let x_mean = 3.0;
        let y_mean = 4.0;
        let epsilon = 0.1;
        let result = noisy_stats(tuples.into_iter(), x_mean, y_mean, epsilon);

        // This is, admittedly, not the greatest test, but it does ensure that noisy_stats
        // is returning values without panicking.
        assert!(!result.is_err());
    }

}
