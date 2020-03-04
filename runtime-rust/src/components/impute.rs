use yarrow_validator::errors::*;

use crate::components::Evaluable;
use yarrow_validator::base::{Value, ArrayND, Vector2DJagged, standardize_numeric_argument, standardize_categorical_argument, standardize_weight_argument, standardize_null_argument, get_argument};
use crate::base::NodeArguments;
use crate::utilities::{noise, utilities};
use ndarray::{ArrayD, arr1};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;
use std::ops::Deref;

impl Evaluable for proto::Impute {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let uniform: String = "Uniform".to_string(); // Distributions
        let gaussian: String = "Gaussian".to_string();

        if arguments.contains_key("categories") {
            match (arguments.get("data").unwrap(), arguments.get("categories").unwrap(), arguments.get("probabilities").unwrap(), arguments.get("null").unwrap()) {
                (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities), Value::Vector2DJagged(nulls)) => Ok(match (data, categories, probabilities, nulls) {
                    (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Bool(nulls)) =>
                        Value::ArrayND(ArrayND::Bool(impute_categorical(&data, &categories, &probabilities, &nulls)?)),
                    (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::F64(nulls)) =>
                        Value::ArrayND(ArrayND::F64(impute_categorical(&data, &categories, &probabilities, &nulls)?)),
                    (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::I64(nulls)) =>
                        Value::ArrayND(ArrayND::I64(impute_categorical(&data, &categories, &probabilities, &nulls)?)),
                    (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Str(nulls)) =>
                        Value::ArrayND(ArrayND::Str(impute_categorical(&data, &categories, &probabilities, &nulls)?)),
                    _ => return Err("types of data, categories, and null must be consistent and probabilities must be f64".into())
                }),
                _ => return Err("data and null must be ArrayND, categories and probabilities must be Vector2DJagged".into())
            }
        } else {
            let distribution = match arguments.get("distribution") {
                Some(distribution) => distribution.deref().to_owned().get_first_str()?,
                None => "Uniform".to_string()
            };

            match &distribution.clone() {
                x if x == &uniform => {
                    return Ok(match (arguments.get("data").unwrap(), arguments.get("min").unwrap(), arguments.get("max").unwrap()) {
                        (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => match (data, min, max) {
                            (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                                Value::ArrayND(ArrayND::F64(impute_float_uniform(&data, &min, &max)?)),
                            (ArrayND::I64(data), ArrayND::I64(_min), ArrayND::I64(_max)) =>
                                // continuous integers are already non-null
                                Value::ArrayND(ArrayND::I64(data.clone())),
                            _ => return Err("data, min, and max must all be the same type".into())
                        },
                        _ => return Err("data, min, max, shift, and scale must be ArrayND".into())
                    })
                },
                x if x == &gaussian => {
                    let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
                    let min = get_argument(&arguments, "min")?.get_arraynd()?.get_f64()?;
                    let max = get_argument(&arguments, "max")?.get_arraynd()?.get_f64()?;
                    let scale = get_argument(&arguments, "scale")?.get_arraynd()?.get_f64()?;
                    let shift = get_argument(&arguments, "shift")?.get_arraynd()?.get_f64()?;

                    return Ok(Value::ArrayND(ArrayND::F64(impute_float_gaussian(&data, &min, &max, &shift, &scale)?)));

                },
                _ => return Err("Distribution not supported".into())
            }
        }
    }
}



/// Given data and min/max values, returns data with imputed values in place of NaN.
/// For now, imputed values are generated uniformly at random between the min and max values provided,
///
/// # Arguments
/// * `data` - data for which you would like to impute the NaN values
/// * `min` - lower bound on imputation range
/// * `max` - upper bound on imputation range
///
/// # Return
/// array of data with imputed values
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::transformations::impute_float_uniform;
/// use core::f64::NAN;
///
/// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
/// let min: f64 = 0.;
/// let max: f64 = 10.;
/// let imputed: ArrayD<f64> = impute_float_uniform(&data, &min, &max)?;
/// println!("{:?}", imputed);
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

/// Given data and min/max values, returns data with imputed values in place of NaN.
/// For now, imputed values are generated uniformly at random between the min and max values provided,
///
/// # Arguments
/// * `data` - data for which you would like to impute the NaN values
/// * `shift` - the mean of the untruncated gaussian noise distribution
/// * `scale` - the standard deviation of the untruncated gaussian noise distribution
/// * `min` - lower bound on imputation range
/// * `max` - upper bound on imputation range
///
/// # Return
/// array of data with imputed values
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::transformations::impute_float_gaussian;
/// use core::f64::NAN;
/// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
/// let shift: f64 = 5.0;
/// let scale: f64 = 7.0;
/// let min: f64 = 0.0;
/// let max: f64 = 10.0;
/// let imputed: ArrayD<f64> = impute_float_gaussian(&data, &shift, &scale, &min, &max)?;
/// println!("{:?}", imputed);
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

pub fn impute_categorical<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>,
                             weights: &Vec<Option<Vec<f64>>>, null_value: &Vec<Option<Vec<T>>>)
                             -> Result<ArrayD<T>> where T:Clone, T:PartialEq, T:Default {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    let categories = standardize_categorical_argument(&categories, &num_columns)?;
    let probabilities = standardize_weight_argument(&categories, &weights, &num_columns)?;
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