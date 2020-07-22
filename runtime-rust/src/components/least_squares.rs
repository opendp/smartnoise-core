/// Beginnings of DP Linear Regression

/// use num_traits::float::{Float, FloatCore};

use core::iter::Iterator;
use displaydoc::Display;
use crate::utilities::mechanisms::laplace_mechanism;

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



/// A module containing the building parts of the main API.
/// You can use these if you want to have more control over the linear regression
mod details {
    use super::Error;
    use num_traits::float::FloatCore;

    /// Low level linear regression primitive for pushing values instead of fetching them
    /// from an iterator
    #[derive(Debug)]
    pub struct Accumulator<F: FloatCore> {
        x_mean: F,
        y_mean: F,
        x_mul_y_mean: F,
        x_squared_mean: F,
        n: usize,
    }

    impl<F: FloatCore> Default for Accumulator<F> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<F: FloatCore> Accumulator<F> {
        pub fn new() -> Self {
            Self {
                x_mean: F::zero(),
                y_mean: F::zero(),
                x_mul_y_mean: F::zero(),
                x_squared_mean: F::zero(),
                n: 0,
            }
        }

        pub fn push(&mut self, x: F, y: F) {
            self.x_mean = self.x_mean + x;
            self.y_mean = self.y_mean + y;
            self.x_mul_y_mean = self.x_mul_y_mean + x * y;
            self.x_squared_mean = self.x_squared_mean + x * x;
            self.n += 1;
        }

        pub fn normalize(&mut self) -> Result<(), Error> {
            match self.n {
                1 => return Ok(()),
                0 => return Err(Error::NoElements),
                _ => {}
            }
            let n = F::from(self.n).ok_or(Error::Mean)?;
            self.n = 1;
            self.x_mean = self.x_mean / n;
            self.y_mean = self.y_mean / n;
            self.x_mul_y_mean = self.x_mul_y_mean / n;
            self.x_squared_mean = self.x_squared_mean / n;
            Ok(())
        }

        pub fn parts(mut self) -> Result<(F, F, F, F), Error> {
            self.normalize()?;
            let Self {
                x_mean,
                y_mean,
                x_mul_y_mean,
                x_squared_mean,
                ..
            } = self;
            Ok((x_mean, y_mean, x_mul_y_mean, x_squared_mean))
        }

        pub fn finish(self) -> Result<(F, F), Error> {
            let (x_mean, y_mean, x_mul_y_mean, x_squared_mean) = self.parts()?;
            let slope = (x_mul_y_mean - x_mean * y_mean) / (x_squared_mean - x_mean * x_mean);
            let intercept = y_mean - slope * x_mean;

            if slope.is_nan() {
                return Err(Error::TooSteep);
            }

            Ok((slope, intercept))
        }
    }

    pub fn lin_reg_imprecise_components<I, F>(xys: I) -> Result<Accumulator<F>, Error>
        where
            F: FloatCore,
            I: Iterator<Item=(F, F)>,
    {
        let mut acc = Accumulator::new();

        for (x, y) in xys {
            acc.push(x, y);
        }

        acc.normalize()?;
        Ok(acc)
    }
}

/// Calculates "NoisyStat", which adds Laplace noise to the OLS sufficient statistics
///
pub fn noisy_stats<I>(xys: I, x_mean: f64, y_mean: f64, epsilon: f64) -> Result<(f64, f64), Error>
    where
        I: Iterator<Item=(f64, f64)>,
{

    let data_size_hint: (usize, Option<usize>) = xys.size_hint();

    let data_size: f64 = data_size_hint.0 as f64;

    let delta: f64 = 1.0f64 - 1.0f64 / data_size;

    let L1: f64 = laplace_mechanism(epsilon, 3.0f64*delta, false).unwrap();
    let L2: f64 = laplace_mechanism(epsilon, 3.0f64*delta, false).unwrap();

    // SUM (x-mean(x))^2
    let mut xxm2 = 0.0f64;

    // SUM (x-mean(x)) (y-mean(y))
    let mut xmym2 = 0.0f64;

    for (x, y) in xys {
        xxm2 = xxm2 + (x - x_mean) * (x - x_mean);
        xmym2 = xmym2 + (x - x_mean) * (y - y_mean);
    }

    let slope = (xmym2 + L1) / (xxm2 + L2);

    let delta_2 = (1.0f64 / f64::from(data_size)) * (1.0f64 + slope.abs());

    let L3: f64 = laplace_mechanism(epsilon, 3.0f64*delta_2, false).unwrap();

    let intercept = y_mean - slope * x_mean;

    let p_25 = 0.25f64 * slope + intercept;
    let p_75 = 0.75f64 * slope + intercept;

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
        let tuples: Vec<(f64, f64)> =
            vec![(1.0, 2.0), (2.0, 4.0), (3.0, 5.0), (4.0, 4.0), (5.0, 5.0)];
        let x_mean = 3.0;
        let y_mean = 4.0;
        assert_!(Ok((0.0, 0.0)), noisy_stats(tuples.into_iter(), x_mean, y_mean, 0.1));
    }

}
