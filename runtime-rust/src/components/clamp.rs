use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, Array, Jagged};
use whitenoise_validator::utilities::{standardize_numeric_argument, standardize_categorical_argument, standardize_null_target_argument, get_argument};
use crate::components::Evaluable;
use ndarray::ArrayD;
use crate::utilities::get_num_columns;
use whitenoise_validator::proto;

impl Evaluable for proto::Clamp {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        // if categories argument was provided, clamp data as if they are categorical (regardless of atomic type)
        if arguments.contains_key("categories") {
            println!("arguments {:?}", arguments);
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "categories")?, get_argument(&arguments, "null")?) {
                (Value::Array(data), Value::Jagged(categories), Value::Array(nulls)) => Ok(match (data, categories, nulls) {
                    (Array::Bool(data), Jagged::Bool(categories), Array::Bool(nulls)) =>
                        clamp_categorical(&data, &categories, &nulls)?.into(),
                    (Array::F64(data), Jagged::F64(categories), Array::F64(nulls)) =>
                        clamp_categorical(&data, &categories, &nulls)?.into(),
                    (Array::I64(data), Jagged::I64(categories), Array::I64(nulls)) =>
                        clamp_categorical(&data, &categories, &nulls)?.into(),
                    (Array::Str(data), Jagged::Str(categories), Array::Str(nulls)) =>
                        clamp_categorical(&data, &categories, &nulls)?.into(),
                    _ => return Err("types of data, categories, and null must be consistent".into())
                }),
                _ => return Err("data must be ArrayND, categories must be Vector2DJagged, and null must be ArrayND".into())
            }
        }
        // if categories argument was not provided, clamp data as numeric
        else {
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "min")?, get_argument(&arguments, "max")?) {
                (Value::Array(data), Value::Array(min), Value::Array(max)) => Ok(match (data, min, max) {
                    (Array::F64(data), Array::F64(min), Array::F64(max)) =>
                        clamp_numeric_float(&data, &min, &max)?.into(),
                    (Array::I64(data), Array::I64(min), Array::I64(max)) =>
                        clamp_numeric_integer(&data, &min, &max)?.into(),
                    _ => return Err("data, min, and max must all have type f64".into())
                }),
                _ => return Err("data, min, and max must all be ArrayND".into())
            }
        }
    }
}

/// Clamps each column of float data to within desired range.
///
/// # Arguments
/// * `data` - Data to be clamped.
/// * `min` - Desired lower bound for each column of the data.
/// * `max` - Desired upper bound for each column of the data.
///
/// # Return
/// Data clamped to desired bounds.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::clamp::clamp_numeric_float;
/// let data = arr2(&[ [1.,2.,3.], [7.,11.,9.] ]).into_dyn();
/// let mins: ArrayD<f64> = arr1(&[0.5, 8., 4.]).into_dyn();
/// let maxes: ArrayD<f64> = arr1(&[2.5, 10., 12.]).into_dyn();
///
/// let clamped_data = clamp_numeric_float(&data, &mins, &maxes).unwrap();
/// assert!(clamped_data == arr2(&[ [1., 8., 4.], [2.5, 10., 9.] ]).into_dyn());
/// ```
pub fn clamp_numeric_float(
    data: &ArrayD<f64>, min: &ArrayD<f64>, max: &ArrayD<f64>
)-> Result<ArrayD<f64>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&min, &num_columns)?.iter())
        .zip(standardize_numeric_argument(&max, &num_columns)?.iter())
        // for each pairing, iterate over the cells
        .for_each(|((mut column, min), max)| column.iter_mut()
            // ignore nan values
            .filter(|v| !v.is_nan())
            // mutate the cell via the operator
            .for_each(|v| *v = min.max(max.min(*v))));

    Ok(data)
}


/// Clamps each column of integral data to within desired range.
///
/// # Arguments
/// * `data` - Data to be clamped.
/// * `min` - Desired lower bound for each column of the data.
/// * `max` - Desired upper bound for each column of the data.
///
/// # Return
/// Data clamped to desired bounds.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::clamp::clamp_numeric_integer;
/// let data = arr2(&[ [1, 2, 3], [7, 11, 9] ]).into_dyn();
/// let mins: ArrayD<i64> = arr1(&[0, 8, 4]).into_dyn();
/// let maxes: ArrayD<i64> = arr1(&[2, 10, 12]).into_dyn();
///
/// let clamped_data = clamp_numeric_integer(&data, &mins, &maxes).unwrap();
/// assert!(clamped_data == arr2(&[ [1, 8, 4], [2, 10, 9] ]).into_dyn());
/// ```
pub fn clamp_numeric_integer(
    data: &ArrayD<i64>, min: &ArrayD<i64>, max: &ArrayD<i64>
)-> Result<ArrayD<i64>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&min, &num_columns)?.iter())
        .zip(standardize_numeric_argument(&max, &num_columns)?.iter())
        // for each pairing, iterate over the cells
        .for_each(|((mut column, min), max)| column.iter_mut()
            // mutate the cell via the operator
            .for_each(|v| *v = *min.max(max.min(v))));

    Ok(data)
}

/// Clamps each column of categorical data to desired set.
///
/// Clamping for categorical data is not as obvious a concept as clamping for numeric data.
/// Clamping takes elements not included in `categories` and maps them to the `null_value`.
///
/// This is useful in the library because having a well-defined set of categories (and a default way
/// to refer to elements outside of this set) is important for common procedures like a
/// differentially private histogram release.
///
/// # Arguments
/// * `data` - Data to be clamped.
/// * `categories` - For each column, the set of categories you want to be represented.
/// * `null_value` - For each column, the value to which elements not included in `categories` will be mapped.
///
/// # Return
/// Data clamped to desired bounds.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::clamp::clamp_categorical;
/// let data: ArrayD<String> = arr2(&[["a".to_string(), "b".to_string(), "3".to_string()],
///                                   ["a".to_string(), "2".to_string(), "b".to_string()]]).into_dyn();
/// let categories: Vec<Option<Vec<String>>> = vec![Some(vec!["a".to_string(), "b".to_string()]),
///                                                 Some(vec!["a".to_string(), "b".to_string()]),
///                                                 Some(vec!["a".to_string(), "b".to_string()])];
/// let null_value: ArrayD<String> = arr1(&["not_a_letter".to_string(),
///                                         "not_a_letter".to_string(),
///                                         "not_a_letter".to_string()]).into_dyn();
///
/// let clamped_data = clamp_categorical(&data, &categories, &null_value).unwrap();
/// assert!(clamped_data == arr2(&[["a".to_string(), "b".to_string(), "not_a_letter".to_string()],
///                                ["a".to_string(), "not_a_letter".to_string(), "b".to_string()]]).into_dyn();)
/// ```
pub fn clamp_categorical<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>, null_value: &ArrayD<T>)
                            -> Result<ArrayD<T>> where T:Clone, T:PartialEq, T:Default {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_categorical_argument(&categories, &num_columns)?)
        .zip(standardize_null_target_argument(&null_value, &num_columns)?)
        // for each pairing, iterate over the cells
        .for_each(|((mut column, categories), null)| column.iter_mut()
            // ignore known values
            .filter(|v| !categories.contains(v))
            // mutate the cell via the operator
            .for_each(|v| *v = null.clone()));

    Ok(data)
}
