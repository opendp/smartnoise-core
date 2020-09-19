use std::cmp::Ordering;
use std::ops::{Add, Div, Mul, Rem, Sub};

use ndarray::{ArrayD, Axis};
use ndarray_stats::interpolate;
use ndarray_stats::QuantileExt;
use noisy_float::types::n64;
use num::{FromPrimitive, ToPrimitive};

use whitenoise_validator::{Float, proto};
use whitenoise_validator::base::{Array, IndexKey, ReleaseNode, Value};
use whitenoise_validator::errors::*;
use whitenoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;
use std::fmt::Debug;

impl Evaluable for proto::Quantile {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?;

        Ok(match arguments.remove::<IndexKey>(&"candidates".into()) {
            Some(candidates) => {
                let lower = arguments.remove(&IndexKey::from("lower"));
                let upper = arguments.remove(&IndexKey::from("upper"));

                match (candidates.array()?, data) {
                    (Array::Float(candidates), Array::Float(data)) =>
                        Value::Array(Array::Float(quantile_utilities_arrayd(
                            candidates.mapv(|v| n64(v as f64)),
                            data.mapv(|v| n64(v as f64)),
                            lower.map(|v| v.array()?.first_float().map(n64)).transpose()?,
                            upper.map(|v| v.array()?.first_float().map(n64)).transpose()?,
                            self.alpha as Float)?)),
                    (Array::Int(candidates), Array::Int(data)) =>
                        Value::Array(Array::Float(quantile_utilities_arrayd(
                            candidates,
                            data,
                            lower.map(|v| v.array()?.first_int()).transpose()?,
                            upper.map(|v| v.array()?.first_int()).transpose()?,
                            self.alpha as Float)?)),
                    _ => return Err("data must be either f64 or i64".into())
                }
            },
            None => match data {
                Array::Float(data) =>
                    quantile(data.mapv(|v| n64(v as f64)), self.alpha, &self.interpolation)?
                        .mapv(|v| v.raw() as Float).into(),
                Array::Int(data) =>
                    quantile(data, self.alpha, &self.interpolation)?.into(),
                _ => return Err("data must be either f64 or i64".into())
            }
        }).map(ReleaseNode::new)
    }
}


/// Accepts data and returns desired quantile of each column in the data.
///
/// # Arguments
/// * `data` - Array of data for which you would like the quantile.
/// * `alpha` - Desired quantile.
///
/// # Return
/// Quantile of interest for each column of your data.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::quantile::quantile;
/// use noisy_float::types::n64;
/// use whitenoise_validator::Float;
/// let data: ArrayD<Float> = arr2(&[ [0., 1., 2.], [2., 3., 4.] ]).into_dyn();
/// let median = quantile(data.mapv(|v| n64(v as f64)), 0.5, &"midpoint".to_string()).unwrap();
/// println!("{:?}", median);
/// assert_eq!(median, arr1(& [1.0, 2.0, 3.0] ).into_dyn().mapv(|v| n64(v as f64)));
/// ```
pub fn quantile<T: FromPrimitive + Ord + Clone + Sub<Output=T> + Mul<Output=T> + Div<Output=T> + Add<Output=T> + Rem<Output=T> + ToPrimitive>(
    mut data: ArrayD<T>, alpha: f64, interpolation: &str
) -> Result<ArrayD<T>> {
    if 0. > alpha || alpha > 1. {
        return Err("q must be within [0, 1]".into());
    }

    match match interpolation.to_lowercase().as_str() {
        "lower" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Lower),
        "upper" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Higher),
        "midpoint" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Midpoint),
        "nearest" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Nearest),
        "linear" => data.quantile_axis_mut(Axis(0), n64(alpha), &interpolate::Linear),
        _ => return Err(format!("interpolation type not recognized: {}", interpolation).into())
    }  {
        Ok(quantiles) => Ok(quantiles),
        Err(_) => Err("unable to compute quantiles".into())
    }
}

pub fn quantile_utilities_arrayd<T: Ord + Clone + Copy + Debug>(
    candidates: ArrayD<T>, data: ArrayD<T>, lower: Option<T>, upper: Option<T>,
    alpha: Float
) -> Result<ArrayD<Float>> {
    Ok(ndarray::Array::from_shape_vec(candidates.shape(), candidates.gencolumns().into_iter()
        .zip(data.gencolumns().into_iter())
        .map(|(candidates, column)|
            quantile_utilities(candidates.to_vec(), column.to_vec(), lower, upper, alpha))
        .collect::<Result<Vec<Vec<_>>>>()?.into_iter()
        .flatten().collect::<Vec<_>>())?.into_dyn())
}


/// Compute median utilities of candidates on a vector
/// Formula is n * max(alpha, 1 - alpha) - |(1 - alpha) * #(Z < r) - alpha * #(Z > r)
///
/// # Arguments
/// * `candidates` - values to be scored
/// * `column` - dataset to score against
/// * `alpha` - parameter for quantile. {0: min, 0.5: median, 1: max, ...}
///
/// # Returns
/// Utility for each candidate
fn quantile_utilities<T: Ord + Clone + Copy + Debug>(
    mut candidates: Vec<T>, mut column: Vec<T>,
    lower: Option<T>, upper: Option<T>, alpha: Float,
) -> Result<Vec<Float>> {
    match (lower, upper) {
        (Some(l), Some(u)) => {
            if l > u { return Err("lower must not be greater than upper".into()) }
            candidates.push(l);
            candidates.push(u);
            column.iter_mut().for_each(|v| *v = l.max(*v).min(u));
        }
        _ => ()
    }
    // sort candidates but preserve original ordering
    let mut candidates = candidates.into_iter().enumerate().collect::<Vec<(usize, T)>>();
    candidates.sort_unstable_by_key(|v| v.1);
    column.sort_unstable();

    let mut col_idx: usize = 0;
    let mut cand_idx: usize = 0;
    let mut utilities = Vec::with_capacity(candidates.len());

    // prepend utilities for candidates less than smallest value of the dataset
    if let Some(v) = column.first() {
        let candidate_score = score_candidate(col_idx, column.len() - col_idx, alpha);
        while cand_idx < candidates.len() && candidates[cand_idx].1 < *v {
            utilities.push(candidate_score);
            cand_idx += 1;
        }
    }

    while cand_idx < candidates.len() && col_idx < column.len() {
        match column[col_idx].cmp(&candidates[cand_idx].1) {
            Ordering::Less => col_idx += 1,
            // if ith value is equal, then there are
            //   i values smaller than the current candidate
            //   loop to find number of values larger than current candidate
            Ordering::Equal => {
                let num_lt = col_idx;
                let num_gt = loop {
                    col_idx += 1;
                    // if all elements are lte, then num_lte == n, so num_gt must be 0
                    if col_idx == column.len() { break column.len() - col_idx }
                    // if next value is greater than candidate,
                    //  then num_gt is n - num_lte
                    if column[col_idx] > candidates[cand_idx].1 {
                        break column.len() - col_idx
                    }
                };
                // score the candidate
                let candidate_score = score_candidate(num_lt, num_gt, alpha);
                // reuse the score for all equivalent candidates
                while cand_idx < candidates.len() && candidates[cand_idx].1 == column[num_lt] {
                    utilities.push(candidate_score);
                    cand_idx += 1;
                }
            }
            // if the ith value is larger, then there are
            //  i values smaller than the current candidate
            //  n - i values larger
            Ordering::Greater => {
                utilities.push(score_candidate(col_idx, column.len() - col_idx, alpha));
                cand_idx += 1;
            }
        }
    }

    // append utilities for candidates greater than the maximum value of the dataset
    let candidate_score = score_candidate(column.len(), 0, alpha);
    utilities.extend((0..candidates.len() - utilities.len()).map(|_| candidate_score));

    // order the utilities by the order of the candidates before they were sorted, and shift the utility
    let constant = alpha.max(1. - alpha);
    Ok(candidates.into_iter().map(|(idx, _)| constant * column.len() as f64 - utilities[idx]).collect())
}

fn score_candidate(num_lt: usize, num_gt: usize, alpha: f64) -> f64 {
    ((1. - alpha) * num_lt as f64 - alpha * num_gt as f64).abs()
}

#[cfg(test)]
mod test_quantile_utilities {
    use ndarray::arr1;
    use noisy_float::types::n64;

    use crate::components::quantile::{quantile_utilities, quantile_utilities_arrayd};

    #[test]
    fn test_scoring() {
        // no candidates, no score
        assert_eq!(
            quantile_utilities::<i64>(vec![], vec![], None, None, 0.5).unwrap(),
            Vec::<f64>::new());
        assert_eq!(
            quantile_utilities(vec![], vec![1], None, None, 0.5).unwrap(),
            Vec::<f64>::new());
        // no data, score should be zero
        assert_eq!(
            quantile_utilities(vec![0], vec![], None, None, 0.5).unwrap(),
            vec![0.]);
        // 0.5 - 0.
        assert_eq!(
            quantile_utilities(vec![0], vec![0], None, None, 0.5).unwrap(),
            vec![0.5]);
        // 0.5 - |0.5 * 0. - 0.5 * 0.|
        // 0.5 - |0.5 * 1. - 0.5 * 0.|
        // 0.5 - |0.5 * 1. - 0.5 * 0.|
        assert_eq!(
            quantile_utilities(vec![0, 1, 2], vec![0], None, None, 0.5).unwrap(),
            vec![0.5, 0., 0.]);
        // 1.5 - |0.5 * 0. - 0.5 * 0.|
        // 1.5 - |0.5 * 3. - 0.5 * 0.|
        // 1.5 - |0.5 * 3. - 0.5 * 0.|
        assert_eq!(
            quantile_utilities(vec![0, 1, 2], vec![0, 0, 0], None, None, 0.5).unwrap(),
            vec![1.5, 0., 0.]);
        // // 1.5 - |0.5 * 0. - 0.5 * 3.|
        // // 1.5 - |0.5 * 0. - 0.5 * 1.|
        // // 1.5 - |0.5 * 0. - 0.5 * 1.|
        assert_eq!(
            quantile_utilities(vec![0, 1, 1], vec![1, 1, 2], None, None, 0.5).unwrap(),
            vec![0., 1., 1.]);
        assert_eq!(
            quantile_utilities(vec![1, 0, 1], vec![2, 1, 1], None, None, 0.5).unwrap(),
            vec![1., 0., 1.]);
    }

    #[test]
    fn utility_arrayd() {
        // 5. is best
        // -10: 12 * 0.5 - |.5 * 0 - .5 * 12| = 0.0
        // -5:  12 * 0.5 - |.5 * 0 - .5 * 12| = 0.0
        // 0:   12 * 0.5 - |.5 * 0 - .5 * 11| = 0.5
        // 2:   12 * 0.5 - |.5 * 1 - .5 * 11| = 1.0
        // 5:   12 * 0.5 - |.5 * 3 - .5 * 6 | = 4.5
        // 7:   12 * 0.5 - |.5 * 8 - .5 * 2 | = 3.0
        // 10:  12 * 0.5 - |.5 * 0 - .5 * 11| = 0.5
        // 12:  12 * 0.5 - |.5 * 0 - .5 * 12| = 0.0
        let utilities = quantile_utilities_arrayd(
            arr1(&[-10., -5., 0., 2., 5., 7., 10., 12.]).into_dyn().mapv(n64),
            arr1(&[0., 10., 5., 7., 6., 4., 3., 8., 7., 6., 5., 5.]).into_dyn().mapv(n64),
            None, None,
            0.5,
        ).unwrap().into_dimensionality::<ndarray::Ix1>().unwrap().to_vec();

        assert_eq!(utilities, vec![0., 0., 0.5, 1.0, 4.5, 3.0, 0.5, 0.]);

        // println!("utilities {:?}", utilities);
    }
}