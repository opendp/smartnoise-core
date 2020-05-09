use whitenoise_validator::errors::*;
use whitenoise_validator::proto;
use whitenoise_validator::base::{Value, Array, Jagged, ReleaseNode};
use whitenoise_validator::utilities::{get_argument, standardize_numeric_argument};

use ndarray::{ArrayD, Axis};

use crate::NodeArguments;
use crate::components::Evaluable;
use crate::utilities::create_subset;
use crate::components::impute::{impute_float_gaussian, impute_float_uniform, impute_categorical};
use crate::utilities::get_num_columns;
use whitenoise_validator::utilities::array::{slow_select, slow_stack};
use std::cmp::Ordering;
use crate::utilities::noise::sample_uniform_int;
use ndarray::prelude::*;
use std::hash::Hash;

impl Evaluable for proto::Resize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mut number_rows = arguments.get("number_rows")
            .and_then(|v| v.first_i64().ok());
        let number_cols = arguments.get("number_columns")
            .and_then(|v| v.first_i64().ok());

        let minimum_rows = arguments.get("minimum_rows")
            .and_then(|v| v.first_i64().ok());

        if let Some(minimum_rows) = minimum_rows {
            number_rows = Some(minimum_rows.clone())
        }

        // If "categories" constraint has been propagated, data are treated as categorical (regardless of atomic type)
        // and imputation (if necessary) is done by sampling from "categories" using the "probabilities" as sampling probabilities for each element.
        if arguments.contains_key("categories") {
            let weights = get_argument(arguments, "weights")
                .and_then(|v| v.jagged()).and_then(|v| v.f64()).ok();

            match (get_argument(arguments, "data")?, get_argument(arguments, "categories")?) {
                // match on types of various arguments and ensure they are consistent with each other
                (Value::Array(data), Value::Jagged(categories)) =>
                    Ok(match (data, categories) {
                        (Array::F64(_), Jagged::F64(_)) =>
                            return Err("categorical resizing over floats in not currently supported- try continuous imputation instead".into()),
//                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        (Array::I64(data), Jagged::I64(categories)) =>
                            resize_categorical(&data, number_rows, number_cols, &categories, &weights, minimum_rows)?.into(),
                        (Array::Bool(data), Jagged::Bool(categories)) =>
                            resize_categorical(&data, number_rows, number_cols, &categories, &weights, minimum_rows)?.into(),
                        (Array::Str(data), Jagged::Str(categories)) =>
                            resize_categorical(&data, number_rows, number_cols, &categories, &weights, minimum_rows)?.into(),
                        _ => return Err("types of data, categories, and nulls must be homogeneous, weights must be f64".into())
                    }),
                _ => return Err("data and nulls must be arrays, categories must be a jagged matrix".into())
            }
        }
        // If "categories" constraint is not populated, data are treated as numeric and imputation (if necessary)
        // is done according to a continuous distribution.
        else {
            match (
                get_argument(arguments, "data")?.array()?,
                get_argument(arguments, "lower")?.array()?,
                get_argument(arguments, "upper")?.array()?
            ) {
                (Array::F64(data), Array::F64(lower), Array::F64(upper)) => {
                    // If there is no valid distribution argument provided, generate uniform by default
                    let distribution = match get_argument(&arguments, "distribution") {
                        Ok(distribution) => distribution.first_string()?,
                        Err(_) => "uniform".to_string()
                    };
                    let shift = match get_argument(arguments, "shift") {
                        Ok(shift) => Some(shift.array()?.f64()?),
                        Err(_) => None
                    };
                    let scale = match get_argument(arguments, "scale") {
                        Ok(scale) => Some(scale.array()?.f64()?),
                        Err(_) => None
                    };
                    Ok(resize_float(data, number_rows, number_cols, &distribution, lower, upper, &shift, &scale, minimum_rows)?.into())
                }
                (Array::I64(data), Array::I64(lower), Array::I64(upper)) =>
                    Ok(resize_integer(data, number_rows, number_cols, lower, upper, minimum_rows)?.into()),
                _ => Err("data, lower, and upper must be of a homogeneous numeric type".into())
            }
        }.map(ReleaseNode::new)
    }
}

/// Resizes data (made up exclusively of f64) based on estimate of n and true size of data.
///
/// Notice that some arguments are denoted with Option<> -- this is because not every distribution used
/// for imputation (if necessary) uses every argument (e.g. Uniform does not use shift or scale).
///
/// NOTE: If more distributions are added here, their corresponding arguments must be added as inputs.
///
/// # Arguments
/// * `data` - The data to be resized
/// * `n` - An estimate of the size of the data -- this could be the guess of the user, or the result of a DP release
/// * `distribution` - The distribution to be used when imputing records
/// * `lower` - A lower bound on data elements
/// * `upper` - An upper bound on data elements
/// * `shift` - The shift (expectation) argument for the Gaussian distribution
/// * `scale` - The scale (standard deviation) argument for the Gaussian distribution
///
/// # Return
/// A resized version of data consistent with the provided `n`
pub fn resize_float(
    data: &ArrayD<f64>,
    number_rows: Option<i64>,
    number_cols: Option<i64>,
    distribution: &String,
    lower: &ArrayD<f64>, upper: &ArrayD<f64>,
    shift: &Option<&ArrayD<f64>>, scale: &Option<&ArrayD<f64>>,
    minimum_rows: Option<i64>
) -> Result<ArrayD<f64>> {
    let mut data = data.clone();

    if let Some(number_cols) = number_cols {

        // get number of columns in actual data
        let real_n = get_num_columns(&data)?;

        data = match real_n.cmp(&number_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[1] = (number_cols - real_n) as usize;
                let synthetic_base = ndarray::ArrayD::from_elem(synthetic_shape, std::f64::NAN).into_dyn();

                // generate synthetic data
                // NOTE: only uniform and gaussian supported at this time
                let synthetic = match distribution.to_lowercase().as_str() {
                    "uniform" => impute_float_uniform(&synthetic_base, &lower, &upper),
                    "gaussian" => impute_float_gaussian(
                        &synthetic_base, &lower, &upper,
                        &shift.cloned().ok_or_else(|| Error::from("shift must be defined for gaussian imputation"))?,
                        &scale.cloned().ok_or_else(|| Error::from("scale must be defined for gaussian imputation"))?),
                    _ => Err("unrecognized distribution".into())
                }?;

                // combine real and synthetic data
                match ndarray::stack(Axis(1), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            Ordering::Greater =>
                data.select(Axis(1), &create_sampling_indices(&number_cols, &real_n)?)
        }
    }

    if let Some(number_rows) = number_rows {

        // get number of observations in actual data
        let real_n: i64 = data.len_of(Axis(0)) as i64;
        if let Some(minimum_rows) = minimum_rows {
            if minimum_rows > real_n {
                return Ok(data.into())
            }
        }

        data = match real_n.cmp(&number_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data,
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (number_rows - real_n) as usize;
                let synthetic_base = ndarray::ArrayD::from_elem(synthetic_shape, std::f64::NAN).into_dyn();

                // generate synthetic data
                // NOTE: only uniform and gaussian supported at this time
                let synthetic = match distribution.to_lowercase().as_str() {
                    "uniform" => impute_float_uniform(&synthetic_base, &lower, &upper),
                    "gaussian" => impute_float_gaussian(
                        &synthetic_base, &lower, &upper,
                        &shift.cloned().ok_or_else(|| Error::from("shift must be defined for gaussian imputation"))?,
                        &scale.cloned().ok_or_else(|| Error::from("scale must be defined for gaussian imputation"))?),
                    _ => Err("unrecognized distribution".into())
                }?;

                // combine real and synthetic data
                match ndarray::stack(Axis(0), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                data.select(Axis(0), &create_sampling_indices(&number_rows, &real_n)?)
        }
    }

    Ok(data)
}


/// Resizes data (made up exclusively of i64) based on estimate of n and true size of data.
///
/// # Arguments
/// * `data` - The data to be resized
/// * `n` - An estimate of the size of the data -- this could be the guess of the user, or the result of a DP release
/// * `lower` - A lower bound on data elements
/// * `upper` - An upper bound on data elements
///
/// # Return
/// A resized version of data consistent with the provided `n`
pub fn resize_integer(
    data: &ArrayD<i64>,
    number_rows: Option<i64>,
    number_cols: Option<i64>,
    lower: &ArrayD<i64>, upper: &ArrayD<i64>,
    minimum_rows: Option<i64>
) -> Result<ArrayD<i64>> {
    let mut data = data.clone();

    if let Some(number_cols) = number_cols {

        // get number of columns in actual data
        let real_n = get_num_columns(&data)?;

        data = match real_n.cmp(&number_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[1] = (number_cols - real_n) as usize;

                let lower = standardize_numeric_argument(lower, &(number_cols - real_n))?
                    .into_dimensionality::<Ix1>()?.to_vec();
                let upper = standardize_numeric_argument(upper, &(number_cols - real_n))?
                    .into_dimensionality::<Ix1>()?.to_vec();

                let mut synthetic = ndarray::ArrayD::zeros(synthetic_shape);
                synthetic.gencolumns_mut().into_iter().zip(lower.into_iter().zip(upper.into_iter()))
                    .map(|(mut column, (min, max))| column.iter_mut()
                        .map(|v| {
                            *v = sample_uniform_int(min, max)?;
                            Ok(())
                        })
                        .collect::<Result<_>>())
                    .collect::<Result<_>>()?;

                // combine real and synthetic data
                match ndarray::stack(Axis(0), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            Ordering::Greater =>
                data.select(Axis(1), &create_sampling_indices(&number_cols, &real_n)?)
        }
    }

    if let Some(number_rows) = number_rows {
        // get number of observations in actual data
        let real_n = data.len_of(Axis(0)) as i64;
        if let Some(minimum_rows) = minimum_rows {
            if minimum_rows > real_n {
                return Ok(data.into())
            }
        }

        data = match &real_n.cmp(&number_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data.clone(),
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (number_rows - real_n) as usize;
                let num_columns = get_num_columns(&data)?;

                let lower = standardize_numeric_argument(lower, &num_columns)?
                    .into_dimensionality::<Ix1>()?.to_vec();
                let upper = standardize_numeric_argument(upper, &num_columns)?
                    .into_dimensionality::<Ix1>()?.to_vec();

                let mut synthetic = ndarray::ArrayD::zeros(synthetic_shape);
                synthetic.gencolumns_mut().into_iter().zip(lower.into_iter().zip(upper.into_iter()))
                    .map(|(mut column, (min, max))| column.iter_mut()
                        .map(|v| {
                            *v = sample_uniform_int(min, max)?;
                            Ok(())
                        })
                        .collect::<Result<_>>())
                    .collect::<Result<_>>()?;

                // combine real and synthetic data
                match ndarray::stack(Axis(0), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                data.select(Axis(0), &create_sampling_indices(&number_rows, &real_n)?)
        }
    }

    Ok(data)
}

/// Resizes categorical data based on estimate of n and true size of data.
///
/// # Arguments
/// * `data` - The data to be resized
/// * `n` - An estimate of the size of the data -- this could be the guess of the user, or the result of a DP release
/// * `categories` - For each data column, the set of possible values for elements in the column
/// * `weights` - For each data column, weights for each category to be used when imputing null values
/// * `null_value` - For each data column, the value of the data to be considered NULL.
///
/// # Return
/// A resized version of data consistent with the provided `n`
pub fn resize_categorical<T>(
    data: &ArrayD<T>,
    number_rows: Option<i64>,
    number_cols: Option<i64>,
    categories: &Vec<Vec<T>>,
    weights: &Option<Vec<Vec<f64>>>,
    minimum_rows: Option<i64>
) -> Result<ArrayD<T>> where T: Clone, T: PartialEq, T: Default, T: Ord, T: Hash {
    let mut data = data.clone();

    if let Some(number_cols) = number_cols {

        // get number of columns in actual data
        let real_n = get_num_columns(&data)?;

        data = match real_n.cmp(&number_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // set synthetic data shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (number_cols - real_n) as usize;

                let num_columns = get_num_columns(&data)?;
                let mut synthetic = ndarray::Array::default(synthetic_shape).into_dyn();

                // iterate over initialized synthetic data and fill with correct null values
                synthetic.gencolumns_mut().into_iter()
                    .for_each(|mut col| col.iter_mut()
                        .for_each(|v| *v = T::default()));

                let null_value = (0..num_columns).map(|_| vec![T::default()]).collect();

                // impute categorical data for each column of nulls to create synthetic data
                synthetic = impute_categorical(
                    &synthetic, &categories, weights, &null_value)?;

                // combine real and synthetic data
                match slow_stack(Axis(0), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            Ordering::Greater =>
                slow_select(&data, Axis(0), &create_sampling_indices(&number_cols, &real_n)?).to_owned(),
        }
    }

    if let Some(number_rows) = number_rows {
        // get number of observations in actual data
        let real_n: i64 = data.len_of(Axis(0)) as i64;
        if let Some(minimum_rows) = minimum_rows {
            if minimum_rows > real_n {
                return Ok(data.into())
            }
        }

        data = match &real_n.cmp(&number_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data.clone(),
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                // set synthetic data shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (number_rows - real_n) as usize;

                let num_columns = get_num_columns(&data)?;
                let mut synthetic = ndarray::Array::default(synthetic_shape).into_dyn();

                // iterate over initialized synthetic data and fill with correct null values
                synthetic.gencolumns_mut().into_iter()
                    .for_each(|mut col| col.iter_mut()
                        .for_each(|v| *v = T::default()));

                let null_value = (0..num_columns).map(|_| vec![T::default()]).collect();

                // impute categorical data for each column of nulls to create synthetic data
                synthetic = impute_categorical(
                    &synthetic, &categories, weights, &null_value)?;

                // combine real and synthetic data
                match slow_stack(Axis(0), &[data.view(), synthetic.view()]) {
                    Ok(value) => value,
                    Err(_) => return Err("failed to stack real and synthetic data".into())
                }
            }
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                slow_select(&data, Axis(0), &create_sampling_indices(&number_rows, &real_n)?).to_owned(),
        }
    }

    Ok(data)
}

/// Accepts size of set (n) and size of desired subset(k) and returns a uniformly drawn
/// set of indices from [1, ..., n] of size k.
///
/// This function is used to create a set of indices that can be used across multiple
/// steps for consistent subsetting.
///
/// # Arguments
///
/// * `k` - The size of the desired subset
/// * `n` - The size of the set from which you want to subset
///
/// # Return
/// A vector of indices representing the subset
///
/// # Example
/// ```
/// use whitenoise_runtime::components::resize::create_sampling_indices;
/// let subset_indices = create_sampling_indices(&5, &10);
/// # subset_indices.unwrap();
/// ```
pub fn create_sampling_indices(k: &i64, n: &i64) -> Result<Vec<usize>> {
    // create set of all indices
    let index_vec: Vec<usize> = (0..(*n as usize)).collect();

    // create uniform selection weights
    let weight_vec: Vec<f64> = vec![1.; *n as usize];

    // create set of sampling indices
    create_subset(&index_vec, &weight_vec, k)
}
