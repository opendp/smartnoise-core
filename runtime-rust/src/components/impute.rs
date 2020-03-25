use whitenoise_validator::errors::*;

use crate::components::Evaluable;
use whitenoise_validator::base::{Value, ArrayND, Vector2DJagged};
use whitenoise_validator::utilities::{standardize_numeric_argument, standardize_categorical_argument, standardize_weight_argument, standardize_null_argument, get_argument};
use crate::base::NodeArguments;
use crate::utilities::{noise, utilities};
use ndarray::{ArrayD};
use crate::utilities::utilities::get_num_columns;
use whitenoise_validator::proto;


impl Evaluable for proto::Impute {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let uniform: String = "Uniform".to_string(); // Distributions
        let gaussian: String = "Gaussian".to_string();

        // if categories argument is not None, treat data as categorical (regardless of atomic type)
        if arguments.contains_key("categories") {
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "categories")?, get_argument(&arguments, "probabilities")?, get_argument(&arguments, "null")?) {
                (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities), Value::Vector2DJagged(nulls)) => Ok(match (data, categories, probabilities, nulls) {
                    (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Bool(nulls)) =>
                        impute_categorical(&data, &categories, &probabilities, &nulls)?.into(),
                    (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::F64(nulls)) =>
                        impute_categorical(&data, &categories, &probabilities, &nulls)?.into(),
                    (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::I64(nulls)) =>
                        impute_categorical(&data, &categories, &probabilities, &nulls)?.into(),
                    (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Str(nulls)) =>
                        impute_categorical(&data, &categories, &probabilities, &nulls)?.into(),
                    _ => return Err("types of data, categories, and null must be consistent and probabilities must be f64".into())
                }),
                _ => return Err("data and null must be ArrayND, categories and probabilities must be Vector2DJagged".into())
            }
        }
        // if categories argument is None, treat data as continuous
        else {
            // get specified data distribution for imputation -- default to Uniform if no valid distribution is provided
            let distribution = match get_argument(&arguments, "distribution") {
                Ok(distribution) => distribution.get_first_str()?,
                Err(_) => "Uniform".to_string()
            };

            match &distribution.clone() {
                // if specified distribution is uniform, identify whether underlying data are of atomic type f64 or i64
                // if f64, impute uniform values
                // if i64, no need to impute (numeric imputation replaces only f64::NAN values, which are not defined for the i64 type)
                x if x == &uniform => {
                    return Ok(match (get_argument(&arguments, "data")?, get_argument(&arguments, "min")?, get_argument(&arguments, "max")?) {
                        (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => match (data, min, max) {
                            (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                                impute_float_uniform(&data, &min, &max)?.into(),
                            (ArrayND::I64(data), ArrayND::I64(_min), ArrayND::I64(_max)) =>
                                // continuous integers are already non-null
                                data.clone().into(),
                            _ => return Err("data, min, and max must all be the same type".into())
                        },
                        _ => return Err("data, min, max, shift, and scale must be ArrayND".into())
                    })
                },
                // if specified distribution is Gaussian, get necessary arguments and impute
                x if x == &gaussian => {
                    let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
                    let min = get_argument(&arguments, "min")?.get_arraynd()?.get_f64()?;
                    let max = get_argument(&arguments, "max")?.get_arraynd()?.get_f64()?;
                    let scale = get_argument(&arguments, "scale")?.get_arraynd()?.get_f64()?;
                    let shift = get_argument(&arguments, "shift")?.get_arraynd()?.get_f64()?;

                    return Ok(impute_float_gaussian(&data, &min, &max, &shift, &scale)?.into());

                },
                _ => return Err("Distribution not supported".into())
            }
        }
    }
}

/// Returns data with imputed values in place of `f64::NAN`.
/// Values are imputed from a uniform distribution.
///
/// # Arguments
/// * `data` - Data for which you would like to impute the `NAN` values.
/// * `min` - Lower bound on imputation range for each column.
/// * `max` - Upper bound on imputation range for each column.
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
/// let min: ArrayD<f64> = arr1(&[0., 2., 3., 4.]).into_dyn();
/// let max: ArrayD<f64> = arr1(&[10., 2., 5., 5.]).into_dyn();
/// let imputed = impute_float_uniform(&data, &min, &max);
/// # imputed.unwrap();
/// ```

pub fn impute_float_uniform(data: &ArrayD<f64>, min: &ArrayD<f64>, max: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&min, &num_columns)?.iter())
        .zip(standardize_numeric_argument(&max, &num_columns)?.iter())
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
/// * `min` - Lower bound on imputation range for each column.
/// * `max` - Upper bound on imputation range for each column.
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
/// let min: ArrayD<f64> = arr1(&[0.0]).into_dyn();
/// let max: ArrayD<f64> = arr1(&[10.0]).into_dyn();
/// let shift: ArrayD<f64> = arr1(&[5.0]).into_dyn();
/// let scale: ArrayD<f64> = arr1(&[7.0]).into_dyn();
/// let imputed = impute_float_gaussian(&data, &min, &max, &shift, &scale);
/// # imputed.unwrap();
/// ```
pub fn impute_float_gaussian(data: &ArrayD<f64>, min: &ArrayD<f64>, max: &ArrayD<f64>, shift: &ArrayD<f64>, scale: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&min, &num_columns)?.iter()
            .zip(standardize_numeric_argument(&max, &num_columns)?.iter()))
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
/// let categories: Vec<Option<Vec<String>>> = vec![Some(vec!["a".to_string(), "c".to_string()]),
///                                                 Some(vec!["b".to_string(), "d".to_string()]),
///                                                 Some(vec!["f".to_string()])];
/// let weights: Vec<Option<Vec<f64>>> = vec![Some(vec![1., 1.]),
///                                           Some(vec![1., 2.]),
///                                           Some(vec![1.])];
/// let null_value: Vec<Option<Vec<String>>> = vec![Some(vec!["null_1".to_string()]),
///                                                 Some(vec!["null_2".to_string()]),
///                                                 Some(vec!["null_3".to_string()])];
///
/// let imputed = impute_categorical(&data, &categories, &weights, &null_value);
/// # imputed.unwrap();
/// ```
pub fn impute_categorical<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>,
                             weights: &Vec<Option<Vec<f64>>>, null_value: &Vec<Option<Vec<T>>>)
                             -> Result<ArrayD<T>> where T:Clone, T:PartialEq, T:Default {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    let categories = standardize_categorical_argument(&categories, &num_columns)?;
    let probabilities = standardize_weight_argument(&categories, &weights)?;
    let null_value = standardize_null_argument(&null_value, &num_columns)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(categories.iter())
        .zip(probabilities.iter())
        .zip(null_value.iter())
        // for each pairing, iterate over the cells
        .map(|(((mut column, cats), probs), null)| column.iter_mut()
            // ignore non null values
            .filter(|v| v == &null)
            // mutate the cell via the operator
            .map(|v| {
                *v = utilities::sample_from_set(&cats, &probs)?;
                Ok(())
            })
            .collect::<Result<()>>())
        .collect::<Result<()>>()?;

    Ok(data)
}