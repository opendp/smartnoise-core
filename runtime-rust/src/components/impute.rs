use whitenoise_validator::errors::*;

use crate::components::Evaluable;
use whitenoise_validator::base::{Value, Array, Jagged, ReleaseNode};
use whitenoise_validator::utilities::{standardize_numeric_argument, standardize_categorical_argument, standardize_weight_argument, get_argument, standardize_null_candidates_argument};
use crate::NodeArguments;
use crate::utilities::{noise};
use crate::utilities;
use ndarray::{ArrayD};
use crate::utilities::get_num_columns;
use whitenoise_validator::proto;
use std::hash::Hash;


impl Evaluable for proto::Impute {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {

        // if categories argument is not None, treat data as categorical (regardless of atomic type)
        if arguments.contains_key("categories") {
            let weights = get_argument(arguments, "weights")
                .and_then(|v| v.jagged()).and_then(|v| v.f64()).ok();

            Ok(ReleaseNode::new(match (
                get_argument(arguments, "data")?.array()?,
                get_argument(arguments, "categories")?.jagged()?,
                get_argument(arguments, "null_values")?.jagged()?) {

                (Array::Bool(data), Jagged::Bool(categories), Jagged::Bool(nulls)) =>
                    impute_categorical(&data, &categories, &weights, &nulls)?.into(),

                (Array::F64(_), Jagged::F64(_), Jagged::F64(_)) =>
                    return Err("categorical imputation over floats is not currently supported".into()),
//                        impute_categorical(&data, &categories, &weights, &nulls)?.into(),

                (Array::I64(data), Jagged::I64(categories), Jagged::I64(nulls)) =>
                    impute_categorical(&data, &categories, &weights, &nulls)?.into(),

                (Array::Str(data), Jagged::Str(categories), Jagged::Str(nulls)) =>
                    impute_categorical(&data, &categories, &weights, &nulls)?.into(),
                _ => return Err("types of data, categories, and null must be consistent and probabilities must be f64".into()),
            }))
        }
        // if categories argument is None, treat data as continuous
        else {
            // get specified data distribution for imputation -- default to Uniform if no valid distribution is provided
            let distribution = match get_argument(arguments, "distribution") {
                Ok(distribution) => distribution.first_string()?,
                Err(_) => "Uniform".to_string()
            };

            match distribution.to_lowercase().as_str() {
                // if specified distribution is uniform, identify whether underlying data are of atomic type f64 or i64
                // if f64, impute uniform values
                // if i64, no need to impute (numeric imputation replaces only f64::NAN values, which are not defined for the i64 type)
                "uniform" => {
                    Ok(match (get_argument(arguments, "data")?, get_argument(arguments, "lower")?, get_argument(arguments, "upper")?) {
                        (Value::Array(data), Value::Array(lower), Value::Array(upper)) => match (data, lower, upper) {
                            (Array::F64(data), Array::F64(lower), Array::F64(upper)) =>
                                impute_float_uniform(&data, &lower, &upper)?.into(),
                            (Array::I64(data), Array::I64(_lower), Array::I64(_upper)) =>
                                // continuous integers are already non-null
                                data.clone().into(),
                            _ => return Err("data, lower, and upper must all be the same type".into())
                        },
                        _ => return Err("data, lower, upper, shift, and scale must be ArrayND".into())
                    })
                },
                // if specified distribution is Gaussian, get necessary arguments and impute
                "gaussian" => {
                    let data = get_argument(arguments, "data")?.array()?.f64()?;
                    let lower = get_argument(arguments, "lower")?.array()?.f64()?;
                    let upper = get_argument(arguments, "upper")?.array()?.f64()?;
                    let scale = get_argument(arguments, "scale")?.array()?.f64()?;
                    let shift = get_argument(arguments, "shift")?.array()?.f64()?;

                    Ok(impute_float_gaussian(&data, &lower, &upper, &shift, &scale)?.into())
                },
                _ => return Err("Distribution not supported".into())
            }.map(ReleaseNode::new)
        }
    }
}

/// Returns data with imputed values in place of `f64::NAN`.
/// Values are imputed from a uniform distribution.
///
/// # Arguments
/// * `data` - Data for which you would like to impute the `NAN` values.
/// * `lower` - Lower bound on imputation range for each column.
/// * `upper` - Upper bound on imputation range for each column.
///
/// # Return
/// Data with `NAN` values replaced with imputed values.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::impute::impute_float_uniform;
/// use core::f64::NAN;
///
/// let data: ArrayD<f64> = arr2(&[ [1., NAN, 3., NAN], [2., 2., NAN, NAN] ]).into_dyn();
/// let lower: ArrayD<f64> = arr1(&[0., 2., 3., 4.]).into_dyn();
/// let upper: ArrayD<f64> = arr1(&[10., 2., 5., 5.]).into_dyn();
/// let imputed = impute_float_uniform(&data, &lower, &upper);
/// # imputed.unwrap();
/// ```

pub fn impute_float_uniform(data: &ArrayD<f64>, lower: &ArrayD<f64>, upper: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&lower, &num_columns)?.iter())
        .zip(standardize_numeric_argument(&upper, &num_columns)?.iter())
        // for each pairing, iterate over the cells
        .map(|((mut column, min), max)| column.iter_mut()
            // ignore nan values
            .filter(|v| v.is_nan())
            // mutate the cell via the operator
            .map(|v| {
                *v = noise::sample_uniform(&min, &max)?;
                Ok(())
            })
            // pool errors
            .collect::<Result<()>>())
        .collect::<Result<()>>()?;

    Ok(data)
}

/// Returns data with imputed values in place of `f64::NAN`.
/// Values are imputed from a truncated Gaussian distribution.
///
/// # Arguments
/// * `data` - Data for which you would like to impute the `NAN` values.
/// * `shift` - The mean of the untruncated Gaussian noise distribution for each column.
/// * `scale` - The standard deviation of the untruncated Gaussian noise distribution for each column.
/// * `lower` - Lower bound on imputation range for each column.
/// * `upper` - Upper bound on imputation range for each column.
///
/// # Return
/// Data with `NAN` values replaced with imputed values.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::impute::impute_float_gaussian;
/// use core::f64::NAN;
/// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
/// let lower: ArrayD<f64> = arr1(&[0.0]).into_dyn();
/// let upper: ArrayD<f64> = arr1(&[10.0]).into_dyn();
/// let shift: ArrayD<f64> = arr1(&[5.0]).into_dyn();
/// let scale: ArrayD<f64> = arr1(&[7.0]).into_dyn();
/// let imputed = impute_float_gaussian(&data, &lower, &upper, &shift, &scale);
/// # imputed.unwrap();
/// ```
pub fn impute_float_gaussian(data: &ArrayD<f64>, lower: &ArrayD<f64>, upper: &ArrayD<f64>, shift: &ArrayD<f64>, scale: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&lower, &num_columns)?.iter()
            .zip(standardize_numeric_argument(&upper, &num_columns)?.iter()))
        .zip(standardize_numeric_argument(&shift, &num_columns)?.iter()
            .zip(standardize_numeric_argument(&scale, &num_columns)?.iter()))
        // for each pairing, iterate over the cells
        .map(|((mut column, (min, max)), (shift, scale))| column.iter_mut()
            // ignore nan values
            .filter(|v| v.is_nan())
            // mutate the cell via the operator
            .map(|v| {
                *v = noise::sample_gaussian_truncated(&min, &max, &shift, &scale)?;
                Ok(())
            })
            .collect::<Result<()>>())
        .collect::<Result<()>>()?;

    Ok(data)
}

/// Returns data with imputed values in place on `null_value`.
///
/// # Arguments
/// * `data` - The data to be resized.
/// * `categories` - For each data column, the set of possible values for elements in the column.
/// * `weights` - For each data column, weights for each category to be used when imputing null values.
/// * `null_value` - For each data column, the value of the data to be considered NULL.
///
/// # Return
/// Data with `null_value` values replaced with imputed values.
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use whitenoise_runtime::components::impute::impute_categorical;
/// let data: ArrayD<String> = arr2(&[["a".to_string(), "b".to_string(), "null_3".to_string()],
///                                   ["c".to_string(), "null_2".to_string(), "a".to_string()]]).into_dyn();
/// let categories: Vec<Vec<String>> = vec![vec!["a".to_string(), "c".to_string()],
///                                         vec!["b".to_string(), "d".to_string()],
///                                         vec!["f".to_string()]];
/// let weights = Some(vec![vec![1., 1.],
///                         vec![1., 2.],
///                         vec![1.]]);
/// let null_value: Vec<Vec<String>> = vec![vec!["null_1".to_string()],
///                                         vec!["null_2".to_string()],
///                                         vec!["null_3".to_string()]];
///
/// let imputed = impute_categorical(&data, &categories, &weights, &null_value);
/// # imputed.unwrap();
/// ```
pub fn impute_categorical<T: Clone>(data: &ArrayD<T>, categories: &Vec<Vec<T>>,
                             weights: &Option<Vec<Vec<f64>>>, null_value: &Vec<Vec<T>>)
                             -> Result<ArrayD<T>> where T:Clone, T:PartialEq, T:Default, T: Ord, T: Hash {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    let categories = standardize_categorical_argument(categories.clone(), &num_columns)?;
    let lengths = categories.iter().map(|cats| cats.len() as i64).collect::<Vec<i64>>();
    let probabilities = standardize_weight_argument(&weights, &lengths)?;
    let null_value = standardize_null_candidates_argument(null_value, &num_columns)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(categories.iter())
        .zip(probabilities.iter())
        .zip(null_value.iter())
        // for each pairing, iterate over the cells
        .map(|(((mut column, cats), probs), null)| column.iter_mut()
            // ignore non null values
            .filter(|v| null.contains(v))
            // mutate the cell via the operator
            .map(|v| {
                *v = utilities::sample_from_set(&cats, &probs)?;
                Ok(())
            })
            .collect::<Result<()>>())
        .collect::<Result<()>>()?;

    Ok(data)
}