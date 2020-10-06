use std::cmp::Ordering;
use std::hash::Hash;

use ndarray::{ArrayD, Axis};
use ndarray::prelude::*;

use whitenoise_validator::{Float, Integer, proto};
use whitenoise_validator::base::{Array, IndexKey, Jagged, ReleaseNode, Value};
use whitenoise_validator::errors::*;
use whitenoise_validator::utilities::{standardize_numeric_argument, take_argument};
use whitenoise_validator::utilities::array::{slow_select, slow_stack};

use crate::components::Evaluable;
use crate::components::impute::{impute_categorical_arrayd, impute_float_gaussian_arrayd, impute_float_uniform_arrayd};
use crate::NodeArguments;
use crate::utilities::create_subset;
use crate::utilities::get_num_columns;
use crate::utilities::noise::{sample_binomial, sample_uniform_int};

pub enum RowResizeConfig {
    NumRows(Integer),
    MinRows(Integer),
    Generalized(Integer, Float),
    None
}

impl Evaluable for proto::Resize {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {

        let number_rows = arguments.remove::<IndexKey>(&"number_rows".into())
            .and_then(|v| v.array().ok()?.first_int().ok()).map(|v| v as i64);
        let number_cols = arguments.remove::<IndexKey>(&"number_columns".into())
            .and_then(|v| v.array().ok()?.first_int().ok()).map(|v| v as i64);
        let minimum_rows = arguments.remove::<IndexKey>(&"minimum_rows".into())
            .and_then(|v| v.array().ok()?.first_int().ok()).map(|v| v as i64);
        let p = arguments.remove::<IndexKey>(&"p".into())
            .and_then(|v| v.array().ok()?.first_float().ok()).map(|v| v as f64);

        // parse options for the number of rows
        let resize_config = match (number_rows, minimum_rows, p) {
            (Some(number_rows), None, Some(p)) => RowResizeConfig::Generalized(number_rows, p),
            (Some(number_rows), None, None) => RowResizeConfig::NumRows(number_rows),
            (None, Some(minimum_rows), None) => RowResizeConfig::MinRows(minimum_rows),
            (None, None, None) => RowResizeConfig::None,
            _ => return Err(Error::from("minimum_rows is exclusive from number_rows and p"))
        };

        // If "categories" constraint has been propagated, data are treated as categorical (regardless of atomic type)
        // and imputation (if necessary) is done by sampling from "categories" using the "probabilities" as sampling probabilities for each element.
        if arguments.contains_key::<IndexKey>(&"categories".into()) {
            let weights = take_argument(&mut arguments, "weights")
                .and_then(|v| v.jagged()).and_then(|v| v.float()).ok();

            match (take_argument(&mut arguments, "data")?, take_argument(&mut arguments, "categories")?) {
                // match on types of various arguments and ensure they are consistent with each other
                (Value::Array(data), Value::Jagged(categories)) =>
                    Ok(match (data, categories) {
                        (Array::Float(_), Jagged::Float(_)) =>
                            return Err("categorical resizing over floats in not currently supported- try continuous imputation instead".into()),
//                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        (Array::Int(data), Jagged::Int(categories)) =>
                            resize_categorical(
                                data, resize_config, number_cols, categories, weights,
                                privacy_definition)?.into(),
                        (Array::Bool(data), Jagged::Bool(categories)) =>
                            resize_categorical(
                                data, resize_config, number_cols, categories, weights,
                                privacy_definition)?.into(),
                        (Array::Str(data), Jagged::Str(categories)) =>
                            resize_categorical(
                                data, resize_config, number_cols, categories, weights,
                                privacy_definition)?.into(),
                        _ => return Err("types of data, categories, and nulls must be homogeneous, weights must be f64".into())
                    }),
                _ => return Err("data and nulls must be arrays, categories must be a jagged matrix".into())
            }
        }
        // If "categories" constraint is not populated, data are treated as numeric and imputation (if necessary)
        // is done according to a continuous distribution.
        else {
            match (
                take_argument(&mut arguments, "data")?.array()?,
                take_argument(&mut arguments, "lower")?.array()?,
                take_argument(&mut arguments, "upper")?.array()?
            ) {
                (Array::Float(data), Array::Float(lower), Array::Float(upper)) => {
                    // If there is no valid distribution argument provided, generate uniform by default
                    let distribution = match take_argument(&mut arguments, "distribution") {
                        Ok(distribution) => distribution.array()?.first_string()?,
                        Err(_) => "uniform".to_string()
                    };
                    let shift = match take_argument(&mut arguments, "shift") {
                        Ok(shift) => Some(shift.array()?.float()?),
                        Err(_) => None
                    };
                    let scale = match take_argument(&mut arguments, "scale") {
                        Ok(scale) => Some(scale.array()?.float()?),
                        Err(_) => None
                    };
                    Ok(resize_float(
                        data, resize_config, number_cols, &distribution,
                        lower, upper, shift, scale,
                        privacy_definition)?.into())
                }
                (Array::Int(data), Array::Int(lower), Array::Int(upper)) =>
                    Ok(resize_integer(
                        data, resize_config, number_cols,
                        lower, upper, privacy_definition)?.into()),
                _ => Err("data, lower, and upper must be of a homogeneous numeric type".into())
            }
        }.map(ReleaseNode::new)
    }
}

fn get_sample_count(p: f64, n_actual: i64, privacy_definition: &proto::PrivacyDefinition) -> Result<i64> {
    use proto::privacy_definition::Neighboring::{self, Substitute, AddRemove};

    let c = p.ceil();
    let s = p / c;

    match Neighboring::from_i32(privacy_definition.neighboring)
        .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))? {
        AddRemove => sample_binomial(c as i64 * n_actual, s, privacy_definition.protect_elapsed_time),
        Substitute => Ok((s * c * n_actual as f64).floor() as i64)
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
/// * `minimum_rows` - The minimum number of rows in the output dataset. Same as number_rows, but will not sample down.
/// * `p` - Maximum proportion of dataset included in output
///
/// # Return
/// A resized version of data consistent with the provided `n`
pub fn resize_float(
    mut data: ArrayD<Float>,
    resize_config: RowResizeConfig,
    number_cols: Option<i64>,
    distribution: &str,
    lower: ArrayD<Float>, upper: ArrayD<Float>,
    shift: Option<ArrayD<Float>>, scale: Option<ArrayD<Float>>,
    privacy_definition: &Option<proto::PrivacyDefinition>
) -> Result<ArrayD<Float>> {

    let enforce_constant_time = privacy_definition.as_ref()
        .map(|v| v.protect_elapsed_time).unwrap_or(false);

    let make_synthetic = |
        shape: Vec<usize>,
        lower: ArrayD<Float>, upper: ArrayD<Float>,
        shift: Option<ArrayD<Float>>, scale: Option<ArrayD<Float>>
    | -> Result<ArrayD<Float>> {
        let synthetic_base: ArrayD<Float> = ndarray::ArrayD::from_elem(shape, Float::NAN).into_dyn();

        // generate synthetic data
        // NOTE: only uniform and gaussian supported at this time
        match distribution.to_lowercase().as_str() {
            "uniform" => impute_float_uniform_arrayd(synthetic_base, lower.clone(), upper.clone(), enforce_constant_time),
            "gaussian" => impute_float_gaussian_arrayd(
                synthetic_base, lower.clone(), upper.clone(),
                shift.ok_or_else(|| Error::from("shift must be defined for gaussian imputation"))?,
                scale.ok_or_else(|| Error::from("scale must be defined for gaussian imputation"))?,
                enforce_constant_time),
            _ => Err("unrecognized distribution".into())
        }
    };

    if let Some(target_num_cols) = number_cols {

        // get number of columns in actual data
        let actual_num_cols = get_num_columns(&data)?;

        data = match actual_num_cols.cmp(&target_num_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[1] = (target_num_cols - actual_num_cols) as usize;
                let synthetic = make_synthetic(
                    synthetic_shape,
                    lower.slice(s![actual_num_cols as usize..]).to_owned().into_dyn(),
                    upper.slice(s![actual_num_cols as usize..]).to_owned().into_dyn(),
                    shift.clone().map(|v| v.slice(s![actual_num_cols as usize..]).to_owned().into_dyn()),
                    scale.clone().map(|v| v.slice(s![actual_num_cols as usize..]).to_owned().into_dyn()))?;

                // combine real and synthetic data
                ndarray::stack(Axis(1), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            }
            Ordering::Greater =>
                data.select(Axis(1), &create_sampling_indices(target_num_cols, actual_num_cols, enforce_constant_time)?)
        }
    }

    let resize_rows = |data: ArrayD<Float>, target_num_rows| {
        let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;

        Ok(match actual_num_rows.cmp(&target_num_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data,
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (target_num_rows - actual_num_rows) as usize;
                let synthetic = make_synthetic(
                    synthetic_shape, lower.clone(), upper.clone(), shift.clone(), scale.clone())?;

                // combine real and synthetic data
                ndarray::stack(Axis(0), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            }
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                data.select(Axis(0), &create_sampling_indices(target_num_rows, actual_num_rows, enforce_constant_time)?)
        })
    };

    match resize_config {
        RowResizeConfig::NumRows(number_rows) => resize_rows(data, number_rows),
        RowResizeConfig::MinRows(minimum_rows) => {
            // get number of observations in actual data
            let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
            if minimum_rows >= actual_num_rows {
                Ok(data)
            } else {
                resize_rows(data, minimum_rows)
            }
        }
        RowResizeConfig::Generalized(number_rows, p) => {
            let real_n: i64 = data.len_of(Axis(0)) as i64;
            let sample_count: i64 = get_sample_count(p, real_n, privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition is required for a sampling resize"))?)?;

            // X_c
            let stacked = ndarray::stack(
                Axis(0), &(0..p.ceil() as i64).map(|_| data.view())
                    .collect::<Vec<ArrayViewD<Float>>>())?;

            // sample(X_c, max(m, n))
            let sample = slow_select(
                &stacked, Axis(0), &create_sampling_indices(
                    number_rows.max(sample_count),
                    stacked.len_of(Axis(0)) as i64, enforce_constant_time)?).to_owned();

            // Aug(phi, max(0, n - m, ...)
            let mut synthetic_shape = stacked.shape().to_vec();
            synthetic_shape[0] = (number_rows - sample_count).max(0) as usize;
            let synthetic = make_synthetic(
                synthetic_shape, lower.clone(), upper.clone(), shift.clone(), scale.clone())?;

            // union(sample, synthetic)
            slow_stack(Axis(0), &[sample.view(), synthetic.view()])
                .map_err(|_| Error::from("failed to stack real and synthetic data"))
        }
        RowResizeConfig::None => Ok(data)
    }
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
    mut data: ArrayD<Integer>,
    resize_config: RowResizeConfig,
    number_cols: Option<i64>,
    lower: ArrayD<Integer>, upper: ArrayD<Integer>,
    privacy_definition: &Option<proto::PrivacyDefinition>
) -> Result<ArrayD<Integer>> {

    let target_num_cols = match number_cols {
        Some(v) => v,
        None => get_num_columns(&data)?
    };

    let lower = standardize_numeric_argument(lower.clone(), target_num_cols)?
        .into_dimensionality::<Ix1>()?.to_vec();
    let upper = standardize_numeric_argument(upper.clone(), target_num_cols)?
        .into_dimensionality::<Ix1>()?.to_vec();

    let enforce_constant_time = privacy_definition.as_ref()
        .map(|v| v.protect_elapsed_time).unwrap_or(false);

    let make_synthetic = |
        shape, lower: &Vec<Integer>, upper: &Vec<Integer>
    | -> Result<ArrayD<Integer>> {

        let mut synthetic = ndarray::ArrayD::zeros(shape);
        synthetic.gencolumns_mut().into_iter().zip(lower.into_iter().zip(upper.into_iter()))
            .try_for_each(|(mut column, (min, max))| column.iter_mut()
                .try_for_each(|v| sample_uniform_int(*min, *max).map(|s| *v = s)))?;
        Ok(synthetic)
    };

    if let Some(target_num_cols) = number_cols {
        let actual_num_cols = get_num_columns(&data)?;

        data = match actual_num_cols.cmp(&target_num_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[1] = (target_num_cols - actual_num_cols) as usize;

                let synthetic = make_synthetic(
                    synthetic_shape,
                    &lower[actual_num_cols as usize..].to_vec(),
                    &upper[actual_num_cols as usize..].to_vec())?;

                // combine real and synthetic data
                ndarray::stack(Axis(1), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            }
            Ordering::Greater =>
                data.select(Axis(1), &create_sampling_indices(target_num_cols, actual_num_cols, enforce_constant_time)?)
        }
    }

    let resize_rows = |data: ArrayD<Integer>, target_num_rows| {
        let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
        Ok(match &actual_num_rows.cmp(&target_num_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data,
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                // initialize synthetic data with correct shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (target_num_rows - actual_num_rows) as usize;
                let synthetic = make_synthetic(synthetic_shape, &lower, &upper)?;

                // combine real and synthetic data
                ndarray::stack(Axis(0), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            }
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                data.select(Axis(0), &create_sampling_indices(target_num_rows, actual_num_rows, enforce_constant_time)?)
        })
    };

    match resize_config {
        RowResizeConfig::NumRows(target_num_rows) => resize_rows(data, target_num_rows),
        RowResizeConfig::MinRows(min_num_rows) => {
            // get number of observations in actual data
            let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
            if min_num_rows > actual_num_rows {
                Ok(data)
            } else {
                resize_rows(data, min_num_rows)
            }
        }
        RowResizeConfig::Generalized(number_rows, p) => {
            let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
            let sample_count: i64 = get_sample_count(p, actual_num_rows, privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition is required for a sampling resize"))?)?;

            // X_c
            let stacked = ndarray::stack(
                Axis(0), &(0..p.ceil() as usize).map(|_| data.view())
                    .collect::<Vec<ArrayViewD<Integer>>>())?;

            // sample(X_c, max(m, n))
            let sample = slow_select(
                &stacked, Axis(0), &create_sampling_indices(
                    number_rows.max(sample_count),
                    stacked.len_of(Axis(0)) as i64, enforce_constant_time)?).to_owned();

            // Aug(phi, max(0, n - m, ...)
            let mut synthetic_shape = stacked.shape().to_vec();
            synthetic_shape[0] = (number_rows - sample_count).max(0) as usize;
            let synthetic = make_synthetic(synthetic_shape, &lower, &upper)?;

            // union(sample, synthetic)
            slow_stack(Axis(0), &[sample.view(), synthetic.view()])
                .map_err(|_| Error::from("failed to stack real and synthetic data"))
        }
        RowResizeConfig::None => Ok(data)
    }
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
    mut data: ArrayD<T>,
    resize_config: RowResizeConfig,
    number_cols: Option<i64>,
    categories: Vec<Vec<T>>,
    weights: Option<Vec<Vec<Float>>>,
    privacy_definition: &Option<proto::PrivacyDefinition>
) -> Result<ArrayD<T>> where T: Clone + PartialEq + Default + Ord + Hash {

    let enforce_constant_time = privacy_definition.as_ref()
        .map(|v| v.protect_elapsed_time).unwrap_or(false);

    let make_synthetic = |
        shape: Vec<usize>, categories: Vec<Vec<T>>, weights: Option<Vec<Vec<Float>>>
    | {
        let mut synthetic = ndarray::Array::default(shape).into_dyn();

        // iterate over initialized synthetic data and fill with correct null values
        synthetic.gencolumns_mut().into_iter()
            .for_each(|mut col| col.iter_mut()
                .for_each(|v| *v = T::default()));

        let null_value = (0..categories.len())
            .map(|_| vec![T::default()])
            .collect::<Vec<Vec<T>>>();

        // impute categorical data for each column of nulls to create synthetic data
        impute_categorical_arrayd(
            synthetic, categories, weights, null_value, enforce_constant_time)
    };

    if let Some(target_num_cols) = number_cols {
        let actual_num_cols = get_num_columns(&data)?;

        data = match actual_num_cols.cmp(&target_num_cols) {
            Ordering::Equal =>
                data,
            Ordering::Less => {
                // set synthetic data shape
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (target_num_cols - actual_num_cols) as usize;

                let actual_num_cols = get_num_columns(&data)?;
                let synthetic = make_synthetic(
                    synthetic_shape,
                    categories[actual_num_cols as usize..].to_vec(),
                    weights.clone().map(|w| w[actual_num_cols as usize..].to_vec()))?;

                // combine real and synthetic data
                slow_stack(Axis(0), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            }
            Ordering::Greater => slow_select(
                &data, Axis(0), &create_sampling_indices(
                    target_num_cols, actual_num_cols, enforce_constant_time)?).to_owned(),
        }
    }

    let resize_rows = |data: ArrayD<T>, target_num_rows| -> Result<ArrayD<T>> {
        let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;

        Ok(match &actual_num_rows.cmp(&target_num_rows) {
            // if estimated n is correct, return real data
            Ordering::Equal =>
                data,
            // if real n is less than estimated n, augment real data with synthetic data
            Ordering::Less => {
                let mut synthetic_shape = data.shape().to_vec();
                synthetic_shape[0] = (target_num_rows - actual_num_rows) as usize;

                let synthetic = make_synthetic(synthetic_shape, categories.clone(), weights.clone())?;
                slow_stack(Axis(0), &[data.view(), synthetic.view()])
                    .map_err(|_| Error::from("failed to stack real and synthetic data"))?
            },
            // if real n is greater than estimated n, return a subset of the real data
            Ordering::Greater =>
                slow_select(&data, Axis(0), &create_sampling_indices(
                    target_num_rows, actual_num_rows, enforce_constant_time)?).to_owned(),
        })
    };

    match resize_config {
        RowResizeConfig::NumRows(target_num_rows) => resize_rows(data, target_num_rows),
        RowResizeConfig::MinRows(min_num_rows) => {
            // get number of observations in actual data
            let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
            if min_num_rows >= actual_num_rows {
                Ok(data)
            } else {
                resize_rows(data, min_num_rows)
            }
        }
        RowResizeConfig::Generalized(number_rows, p) => {
            let actual_num_rows: i64 = data.len_of(Axis(0)) as i64;
            let sample_count: i64 = get_sample_count(p, actual_num_rows, privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition is required for a sampling resize"))?)?;

            // X_c
            let stacked = slow_stack(
                Axis(0), &(0..p.ceil() as usize).map(|_| data.view())
                    .collect::<Vec<ArrayViewD<T>>>())?;

            // sample(X_c, max(m, n))
            let sample = slow_select(
                &stacked, Axis(0), &create_sampling_indices(
                    number_rows.max(sample_count),
                    stacked.len_of(Axis(0)) as i64, enforce_constant_time)?).to_owned();

            // Aug(phi, max(0, n - m, ...)
            let mut synthetic_shape = stacked.shape().to_vec();
            synthetic_shape[0] = (number_rows - sample_count).max(0) as usize;
            let synthetic = make_synthetic(synthetic_shape, categories, weights)?;

            // union(sample, synthetic)
            slow_stack(Axis(0), &[sample.view(), synthetic.view()])
                .map_err(|_| Error::from("failed to stack real and synthetic data"))
        }
        RowResizeConfig::None => Ok(data)
    }
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
/// let subset_indices = create_sampling_indices(5, 10, false);
/// # subset_indices.unwrap();
/// ```
pub fn create_sampling_indices(k: i64, n: i64, enforce_constant_time: bool) -> Result<Vec<usize>> {
    // create set of all indices
    let index_vec: Vec<usize> = (0..(n as usize)).collect();

    // create uniform selection weights
    let weight_vec: Vec<f64> = vec![1.; n as usize];

    // create set of sampling indices
    create_subset(&index_vec, &weight_vec, k as usize, enforce_constant_time)
}
