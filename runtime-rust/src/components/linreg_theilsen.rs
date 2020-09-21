use indexmap::indexmap;
use ndarray;

use whitenoise_validator::{Float, Integer, proto};
use whitenoise_validator::base::{ReleaseNode, Value};
use whitenoise_validator::errors::*;
use whitenoise_validator::utilities::take_argument;

use crate::components::Evaluable;
use crate::NodeArguments;
use proto::privacy_definition::Neighboring;
use crate::utilities::noise::shuffle;


impl Evaluable for proto::TheilSen {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        // theil-sen inputs must be 1d
        let data_x = take_argument(&mut arguments, "data_x")?
            .array()?.float()?.into_dimensionality::<ndarray::Ix1>()?.to_vec();
        let data_y = take_argument(&mut arguments, "data_y")?
            .array()?.float()?.into_dimensionality::<ndarray::Ix1>()?.to_vec();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;
        let neighboring = Neighboring::from_i32(privacy_definition.neighboring)
            .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;
        let enforce_constant_time = privacy_definition.protect_elapsed_time;

        let (slopes, intercepts) = match self.implementation.to_lowercase().as_str() {
            "theil-sen" => theil_sen_transform(&data_x, &data_y, neighboring),
            "theil-sen-k-match" => theil_sen_transform_k_match(
                &data_x, &data_y,
                take_argument(&mut arguments, "k")?.array()?.first_int()?,
                neighboring, enforce_constant_time),
            _ => return Err(Error::from("Invalid implementation"))
        }?;

        Ok(ReleaseNode::new(Value::Dataframe(indexmap![
            "slopes".into() => ndarray::Array::from(slopes).into_dyn().into(),
            "intercepts".into() => ndarray::Array::from(intercepts).into_dyn().into()
        ])))
    }
}

/// Calculate slope between two points
///
fn compute_slope(x: &(Float, Float), y: &(Float, Float)) -> Float {
    (y.1 - y.0) / (x.1 - x.0)
}

/// Calculate y intercept from two points and a slope
///
fn compute_intercept(x: &(Float, Float), y: &(Float, Float), slope: Float) -> Float {
    (y.0 + y.1) / 2. - slope * (x.0 + x.1) / 2.
}

/// Compute parameters between all pairs of points where defined
///
pub fn theil_sen_transform(
    x: &Vec<Float>, y: &Vec<Float>,
    neighboring: Neighboring
) -> Result<(Vec<Float>, Vec<Float>)> {
    if x.len() != y.len() {
        return Err("predictors and targets must share same length".into())
    }

    let n = x.len();
    let mut slopes: Vec<Float> = Vec::new();
    let mut intercepts: Vec<Float> = Vec::new();

    for p in 0..n as usize {
        for q in p + 1..n as usize {
            let x_pair = (x[p], x[q]);
            let y_pair = (y[p], y[q]);

            let slope = compute_slope(&x_pair, &y_pair);
            if neighboring == Neighboring::AddRemove && !slope.is_finite() {
                continue
            }
            slopes.push(slope);
            intercepts.push(compute_intercept(&x_pair, &y_pair, slope));
        }
    }
    Ok((slopes, intercepts))
}

/// Implementation from paper
/// Separate data into two bins, match members of each bin to form pairs
/// Note: k is number of trials here
pub fn theil_sen_transform_k_match(
    x: &Vec<Float>, y: &Vec<Float>, k: Integer,
    neighboring: Neighboring,
    enforce_constant_time: bool
) -> Result<(Vec<Float>, Vec<Float>)> {
    if x.len() != y.len() {
        return Err("x and y must be the same length".into())
    }

    let n = x.len();
    let mut slopes: Vec<Float> = Vec::new();
    let mut intercepts: Vec<Float> = Vec::new();

    for _iteration in 0..k {
        let shuffled: Vec<(Float, Float)> = shuffle(x.iter().copied()
            .zip(y.iter().copied()).collect(), enforce_constant_time)?;

        // For n odd, the last data point in "shuffled" will be ignored
        let midpoint = n / 2;

        for i in 0..midpoint {
            let x_pair = (shuffled[i].0, shuffled[midpoint + i].0);
            let y_pair = (shuffled[i].1, shuffled[midpoint + i].1);

            let slope = compute_slope(&x_pair, &y_pair);
            if neighboring == Neighboring::AddRemove && !slope.is_finite() {
                continue
            }
            slopes.push(slope);
            intercepts.push(compute_intercept(&x_pair, &y_pair, slope));
        }
    }

    Ok((slopes, intercepts))
}

#[cfg(test)]
pub mod tests {
    use crate::utilities::noise;

    use super::*;

    pub fn test_dataset(n: Integer) -> (Vec<Float>, Vec<Float>) {
        let x = (0..n).map(|i| i as f64 + noise::sample_gaussian(0., 0.1, false).unwrap()).collect();
        let y = (0..n).map(|i| (2 * i) as f64 + noise::sample_gaussian(0., 0.1, false).unwrap()).collect();
        (x, y)
    }

    pub fn median(x: &Vec<Float>) -> Float {
        let mut tmp: Vec<Float> = x.clone();
        tmp.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = tmp.len() / 2;
        if tmp.len() % 2 == 0 {
            (tmp[mid - 1] + tmp[mid]) / 2.0
        } else {
            tmp[mid]
        }
    }

    /// Non-DP implementation of Theil-Sen to test DP version against
    ///
    pub fn public_theil_sen(x: &Vec<Float>, y: &Vec<Float>) -> (Float, Float) {

        // Slope m is median of slope calculated between all pairs of
        // non-identical points
        let (slopes, intercepts) = theil_sen_transform(x, y, Neighboring::AddRemove).unwrap();
        let slope = median(&slopes);
        let intercept = median(&intercepts);

        return (slope, intercept)
    }

    #[test]
    fn theil_sen_length() {
        let (x, y) = test_dataset(10);
        let (slopes, intercepts) = theil_sen_transform(&x, &y, Neighboring::AddRemove).unwrap();

        let n = x.len() as Integer;
        assert_eq!(slopes.len() as Integer, n * (n - 1) / 2);
        assert_eq!(intercepts.len() as Integer, n * (n - 1) / 2);
    }

    #[test]
    fn theil_sen_value() {
        // Ensure non-DP version gives y = 2x for this data
        let (x, y) = test_dataset(10);
        let (slope, intercept) = public_theil_sen(&x, &y);
        assert!((2.0 - slope).abs() <= 0.1);
        assert!((0.0 - intercept).abs() <= 0.1);
    }


    // MS: I busted this test
    // #[test]
    // fn intercept_estimation_test() {
    //
    //     let (x, y) = test_dataset(1000);
    //     x.into_iter().tuple_windows()
    //         .zip(y.into_iter().tuple_windows())
    //         .map(|(x, y)| compute_intercept(&x, &y, 2.0))
    //         .for_each(|intercept| assert!(intercept.abs() <= 5.0));
    // }
}
