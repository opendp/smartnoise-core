use whitenoise_validator::errors::*;
use whitenoise_validator::proto;
use whitenoise_validator::base::{Value, ArrayND, Vector2DJagged};
use whitenoise_validator::utilities::get_argument;

use ndarray::{ArrayD, Axis, Array};
use rug::{Float, ops::Pow};

use crate::base::NodeArguments;
use crate::components::Evaluable;
use crate::utilities::noise;
use crate::components::impute::{impute_float_gaussian, impute_float_uniform, impute_categorical};
use crate::utilities::utilities::get_num_columns;
use whitenoise_validator::utilities::array::{slow_select, slow_stack};

impl Evaluable for proto::Resize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let n = get_argument(&arguments, "n")?.get_first_i64()?;

        // If "categories" constraint has been propagated, data are treated as categorical (regardless of atomic type)
        // and imputation (if necessary) is done by sampling from "categories" using the "probabilities" as sampling probabilities for each element.
        if arguments.contains_key("categories") {
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "categories")?,
                   get_argument(&arguments, "probabilities")?) {
                // match on types of various arguments and ensure they are consistent with each other
                (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities)) =>
                    Ok(match (data, categories, probabilities) {
                        (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities)) =>
                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities)) =>
                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities)) =>
                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::F64(probabilities)) =>
                            resize_categorical(&data, &n, &categories, &probabilities)?.into(),
                        _ => return Err("types of data, categories, and nulls must be homogeneous, probabilities must be f64".into())
                    }),
                _ => return Err("data and nulls must be arrays, categories must be a jagged matrix".into())
            }
        }
        // If "categories" constraint is not populated, data are treated as numeric and imputation (if necessary)
        // is done according to a continuous distribution.
        else {
            // If there is no valid distribution argument provided, generate uniform by default
            let distribution = match get_argument(&arguments, "type") {
                Ok(distribution) => distribution.get_first_str()?,
                Err(_) => "Uniform".to_string()
            };
            let shift = match get_argument(&arguments, "shift") {
                Ok(shift) => Some(shift.get_arraynd()?.get_f64()?),
                Err(_) => None
            };
            let scale = match get_argument(&arguments, "scale") {
                Ok(scale) => Some(scale.get_arraynd()?.get_f64()?),
                Err(_) => None
            };
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "min")?, get_argument(&arguments, "max")?) {
                // TODO: add support for resizing ints
                (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => match (data, min, max) {
                    (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                        Ok(Value::ArrayND(ArrayND::F64(resize_float(&data, &n, &distribution, &min, &max, &shift, &scale)?))),
                    _ => Err("data, min, and max must all be of float type".into())
                },
                _ => Err("data, min, and max must all be arrays".into())
            }
        }
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
/// * `min` - A lower bound on data elements
/// * `max` - An upper bound on data elements
/// * `shift` - The shift (expectation) argument for the Gaussian distribution
/// * `scale` - The scale (standard deviation) argument for the Gaussian distribution
///
/// # Return
/// A resized version of data consistent with the provided `n`
pub fn resize_float(data: &ArrayD<f64>, n: &i64, distribution: &String,
                    min: &ArrayD<f64>, max: &ArrayD<f64>,
                    shift: &Option<&ArrayD<f64>>, scale: &Option<&ArrayD<f64>>) -> Result<ArrayD<f64>> {
    // get number of observations in actual data
    let real_n: i64 = data.len_of(Axis(0)) as i64;

    Ok(match &real_n {
        // if estimated n is correct, return real data
        real_n if real_n == n =>
            data.clone(),
        // if estimated n is larger than real n, augment real data with synthetic data
        real_n if real_n < n => {
            // initialize synthetic data with correct shape
            let mut synthetic_shape = data.shape().to_vec();
            synthetic_shape[0] = (n - real_n) as usize;
            let synthetic_base = Array::from_elem(synthetic_shape, std::f64::NAN).into_dyn();

            // generate synthetic data
            // NOTE: only uniform and gaussian supported at this time
            let synthetic = match distribution.as_str() {
                "Uniform" => impute_float_uniform(&synthetic_base, &min, &max),
                "Gaussian" => impute_float_gaussian(
                    &synthetic_base, &min, &max,
                    &shift.cloned().ok_or::<Error>("shift must be defined for gaussian imputation".into())?,
                    &scale.cloned().ok_or::<Error>("scale must be defined for gaussian imputation".into())?),
                _ => Err("unrecognized distribution".into())
            }?;

            // combine real and synthetic data
            match ndarray::stack(Axis(0), &[data.view(), synthetic.view()]) {
                Ok(value) => value,
                Err(_) => return Err("failed to stack real and synthetic data".into())
            }
        }
        // if estimated n is smaller than real n, return a subset of the real data
        real_n if real_n > n =>
            slow_select(&data, Axis(0), &create_sampling_indices(&n, &real_n)?),
//            data.select(Axis(0), &create_sampling_indices(&n, &real_n)?).to_owned(),
        _ => return Err("invalid configuration for n when resizing".into())
    })
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
    data: &ArrayD<T>, n: &i64,
    categories: &Vec<Option<Vec<T>>>,
    weights: &Vec<Option<Vec<f64>>>,
) -> Result<ArrayD<T>> where T: Clone, T: PartialEq, T: Default {
    // get number of observations in actual data
    let real_n: i64 = data.len_of(Axis(0)) as i64;

    Ok(match &real_n {
        // if estimated n is correct, return real data
        real_n if real_n == n =>
            data.clone(),
        // if estimated n is larger than real n, augment real data with synthetic data
        real_n if real_n < n => {
            // set synthetic data shape
            let mut synthetic_shape = data.shape().to_vec();
            synthetic_shape[0] = (n - real_n) as usize;

            let num_columns = get_num_columns(&data)?;
            let mut synthetic = Array::default(synthetic_shape).into_dyn();

            // iterate over initialized synthetic data and fill with correct null values
            synthetic.gencolumns_mut().into_iter()
                .for_each(|mut col| col.iter_mut()
                    .for_each(|v| *v = T::default()));

            let null_value = (0..num_columns).map(|_| Some(vec![T::default()])).collect();

            // impute categorical data for each column of nulls to create synthetic data
            synthetic = impute_categorical(
                &synthetic, &categories, &weights, &null_value)?;

            // combine real and synthetic data
            match slow_stack(Axis(0), &[data.view(), synthetic.view()]) {
                Ok(value) => value,
                Err(_) => return Err("failed to stack real and synthetic data".into())
            }
        }
        // if estimated n is smaller than real n, return a subset of the real data
        real_n if real_n > n =>
            slow_select(data, Axis(0), &create_sampling_indices(&n, &real_n)?).to_owned(),
        _ => return Err("invalid configuration for n when resizing".into())
    })
}

/// Accepts set and element weights and returns a subset of size k (without replacement).
///
/// Weights are (after being normalized) the probability of drawing each element on the first draw (they sum to 1)
/// Based on Algorithm A from Raimidis PS, Spirakis PG (2006). “Weighted random sampling with a reservoir.”
///
/// # Arguments
/// * `set` - Set of elements for which you would like to create a subset
/// * `weights` - Weight for each element in the set, corresponding to the probability it is drawn on the first draw.
/// * `k` - The size of the desired subset
///
/// # Return
/// subset of size k sampled according to weights
///
/// # Example
/// ```
/// use whitenoise_runtime::components::resize::create_subset;
/// let set = vec![1, 2, 3, 4, 5, 6];
/// let weights = vec![1., 1., 1., 2., 2., 2.];
/// let k = 3;
/// let subset = create_subset(&set, &weights, &k);
/// # subset.unwrap();
/// ```
pub fn create_subset<T>(set: &Vec<T>, weights: &Vec<f64>, k: &i64) -> Result<Vec<T>> where T: Clone {
    if *k as usize > set.len() { return Err("k must be less than the set length".into()); }

    // generate sum of weights
    let weights_rug: Vec<rug::Float> = weights.into_iter().map(|w| Float::with_val(53, w)).collect();
    let weights_sum: rug::Float = Float::with_val(53, Float::sum(weights_rug.iter()));

    // convert weights to probabilities
    let probabilities: Vec<rug::Float> = weights_rug.iter().map(|w| w / weights_sum.clone()).collect();

    // initialize subsample
    let _subsample_vec: Vec<T> = Vec::with_capacity(*k as usize);

    //
    // generate keys and identify top k indices
    //

    // generate key/index tuples
    let mut key_vec = Vec::with_capacity(*k as usize);
    for i in 0..*k {
        key_vec.push((noise::sample_uniform_mpfr(0., 1.)?.pow(1. / probabilities[i as usize].clone()), i));
    }

    // sort key/index tuples by key and identify top k indices
    key_vec.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let mut top_indices: Vec<i64> = Vec::with_capacity(*k as usize);
    for i in 0..*k {
        top_indices.push(key_vec[i as usize].1 as i64);
    }

    // subsample based on top k indices
    let mut subset: Vec<T> = Vec::with_capacity(*k as usize);
    for value in top_indices.iter().map(|&index| set[index as usize].clone()) {
        subset.push(value);
    }

    Ok(subset)
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
    let index_vec: Vec<usize> = (0..*n).map(|v| v as usize).collect();

    // create uniform selection weights
    let weight_vec: Vec<f64> = vec![1.; *n as usize];

    // create set of sampling indices
    create_subset(&index_vec, &weight_vec, k)
}
