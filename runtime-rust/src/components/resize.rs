use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use crate::components::Evaluable;
use yarrow_validator::base::{Value, ArrayND, Vector2DJagged, get_argument, standardize_null_argument};

use ndarray::{ArrayD, Axis, Array};
use rug::{Float, ops::Pow};

use crate::utilities::noise;
use crate::components::impute::{impute_float_gaussian, impute_float_uniform, impute_categorical};
use yarrow_validator::proto;

use crate::utilities::utilities::get_num_columns;
use crate::utilities::array::{select, stack};

impl Evaluable for proto::Resize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let n = get_argument(&arguments, "n")?.get_first_i64()?;

        if arguments.contains_key("categories") {
            match (get_argument(&arguments, "data")?, get_argument(&arguments, "categories")?,
                   get_argument(&arguments, "probabilities")?, get_argument(&arguments, "null")?) {
                (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities), Value::Vector2DJagged(nulls)) =>
                    Ok(match (data, categories, probabilities, nulls) {
                        (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::F64(nulls)) =>
                            resize_categorical(&data, &n, &categories, &probabilities, &nulls)?.into(),
                        (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::I64(nulls)) =>
                            resize_categorical(&data, &n, &categories, &probabilities, &nulls)?.into(),
                        (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Bool(nulls)) =>
                            resize_categorical(&data, &n, &categories, &probabilities, &nulls)?.into(),
                        (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Str(nulls)) =>
                            resize_categorical(&data, &n, &categories, &probabilities, &nulls)?.into(),
                        _ => return Err("types of data, categories and nulls must be homogenous, probabilities must be f64".into())
                    }),
                _ => return Err("data and nulls must be arrays, categories must be a jagged matrix".into())
            }
        } else {
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
                    _ => Err("data, min and max must all be of float type".into())
                },
                _ => Err("data, min and max must all be arrays".into())
            }
        }
    }
}

pub fn resize_float(data: &ArrayD<f64>, n: &i64, distribution: &String,
                    min: &ArrayD<f64>, max: &ArrayD<f64>,
                    shift: &Option<&ArrayD<f64>>, scale: &Option<&ArrayD<f64>>) -> Result<ArrayD<f64>> {
    // get number of observations in actual data
    let real_n: i64 = data.len_of(Axis(0)) as i64;

    Ok(match &real_n {
        real_n if real_n == n =>
            data.clone(),
        real_n if real_n < n => {
            let mut synthetic_shape = data.shape().to_vec();
            synthetic_shape[0] = (n - real_n) as usize;

            let synthetic_base = Array::from_elem(synthetic_shape, std::f64::NAN).into_dyn();

            let synthetic = match distribution.as_str() {
                "Uniform" => impute_float_uniform(&synthetic_base, &min, &max),
                "Gaussian" => impute_float_gaussian(
                        &synthetic_base, &min, &max,
                        &shift.cloned().ok_or::<Error>("shift must be defined for gaussian imputation".into())?,
                        &scale.cloned().ok_or::<Error>("scale must be defined for gaussian imputation".into())?),
                _ => Err("unrecognized distribution".into())
            }?;

            match stack(Axis(0), &[data.view(), synthetic.view()]) {
                Ok(value) => value,
                Err(_) => return Err("failed to stack real and synthetic data".into())
            }
        },
        real_n if real_n > n =>
            select(&data, Axis(0), &create_sampling_indices(&n, &real_n)?),
//            data.select(Axis(0), &create_sampling_indices(&n, &real_n)?).to_owned(),
        _ => return Err("invalid configuration for n when resizing".into())
    })
}

pub fn resize_categorical<T>(data: &ArrayD<T>, n: &i64,
                             categories: &Vec<Option<Vec<T>>>, weights: &Vec<Option<Vec<f64>>>, null_value: &Vec<Option<Vec<T>>>,)
                             -> Result<ArrayD<T>> where T: Clone, T: PartialEq, T: Default {
    // get number of observations in actual data
    let real_n: i64 = data.len_of(Axis(0)) as i64;

    Ok(match &real_n {
        real_n if real_n == n =>
            data.clone(),
        real_n if real_n < n => {
            let mut synthetic_shape = data.shape().to_vec();
            synthetic_shape[0] = (n - real_n) as usize;

            let num_columns = get_num_columns(&data)?;
            let mut synthetic = Array::default(synthetic_shape).into_dyn();

            synthetic.gencolumns_mut().into_iter()
                .zip(standardize_null_argument(&null_value, &num_columns)?.iter())
                .for_each(|(mut col, null)| col.iter_mut()
                    .for_each(|v| *v = null.clone()));

            synthetic = impute_categorical(
                &synthetic, &categories, &weights, &null_value)?;

            match stack(Axis(0), &[data.view(), synthetic.view()]) {
                Ok(value) => value,
                Err(_) => return Err("failed to stack real and synthetic data".into())
            }
        },
        real_n if real_n > n =>
            select(data, Axis(0), &create_sampling_indices(&n, &real_n)?).to_owned(),
        _ => return Err("invalid configuration for n when resizing".into())
    })
}

/// Accepts set and element probabilities and returns a subset of size k
/// Probabilities are the probability of drawing each element on the first draw (they sum to 1)
/// Based on Algorithm A from Raimidis PS, Spirakis PG (2006). “Weighted random sampling with a reservoir.”
pub fn create_subset<T>(set: &Vec<T>, weights: &Vec<f64>, k: &i64) -> Result<Vec<T>> where T: Clone {

    if *k as usize > set.len() {return Err("k must be less than the set length".into())}

    // let weights_sum: f64 = weights.iter().sum();

    // let probabilities_vec: Vec<f64> = weights.iter().map(|w| w / weights_sum).collect();

    // generate sum of weights
    let weights_rug: Vec<rug::Float> = weights.into_iter().map(|w| Float::with_val(53, w)).collect();
    let weights_sum: rug::Float = Float::with_val(53, Float::sum(weights_rug.iter()));

    // convert weights to probabilities
    let probabilities: Vec<rug::Float> = weights_rug.iter().map(|w| w / weights_sum.clone()).collect();

    let _subsample_vec: Vec<T> = Vec::with_capacity(*k as usize);

    //
    // generate keys and identify top k indices
    //

    // generate key/index tuples
    let mut key_vec = Vec::with_capacity(*k as usize);
    for i in 0..*k {
        key_vec.push((noise::mpfr_uniform(0., 1.)?.pow(1. / probabilities[i as usize].clone()), i));
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

pub fn create_sampling_indices(k: &i64, n: &i64) -> Result<Vec<usize>> {
    /// Creates set of indices for subsampling from data without replacement

    // create set of all indices
    let index_vec: Vec<usize> = (0..*n).map(|v| v as usize).collect();

    // create uniform selection probabilities
    let prob_vec: Vec<f64> = vec![1./(*n as f64); *n as usize];

    // create set of sampling indices
    create_subset(&index_vec, &prob_vec, k)
}
