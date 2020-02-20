use std::string::String;
use std::vec::Vec;
use std::cmp;
use ndarray::prelude::*;
use ndarray::stack;
use core::f64::NAN;
use num;

use crate::utilities::noise;
use crate::utilities::aggregations;

/// Accepts vector of data and assigns each element to a bin
/// NOTE: bin transformation has C-stability of 1
///
/// # Arguments
/// * `data` - Array of numeric data to be binned
/// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
/// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
///                      If false, then bins are closed on the right.
///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
///                      regardless of the value of `inclusive_left`.
///
/// # Return
/// ArrayD of bin assignments
///
/// # Example
/// ```
/// // load crates
/// use std::string::String;
/// use std::vec::Vec;
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::transformations::bin;
///
/// // set up data
/// let data: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5., 12., 19., 24., 90., 98.]).into_dyn();
/// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
/// let inclusive_left: bool = false;
///
/// // bin data
/// let binned_data: ArrayD<String> = bin(&data, &edges, &inclusive_left);
/// println!("{:?}", binned_data);
/// ```
pub fn bin(data: &ArrayD<f64>, edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {

    // create vector versions of data and edges
    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let mut sorted_edges: Vec<f64> = edges.clone().into_dimensionality::<Ix1>().unwrap().to_vec();

    //  ensure edges are sorted in ascending order
    sorted_edges.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // initialize output vector
    let mut bin_vec: Vec<String> = Vec::with_capacity(data_vec.len());

    // for each data element, look for correct bin and append name to bin_vec
    for i in 0..data_vec.len() {
        // append empty string if data are outside of bin ranges
        if data_vec[i] < sorted_edges[0] || data_vec[i] > sorted_edges[sorted_edges.len()-1] {
            bin_vec.push("".to_string());
        } else {
            // for each bin
            for j in 0..(sorted_edges.len()-1) {
                if  // element is less than the right bin edge
                    data_vec[i] < sorted_edges[j+1] ||
                    // element is equal to the right bin edge and we are building our histogram to be 'right-edge inclusive'
                    (data_vec[i] == sorted_edges[j+1] && inclusive_left == &false) ||
                    // element is equal to the right bin edge and we are checking our rightmost bin
                    (data_vec[i] == sorted_edges[j+1] && j == (sorted_edges.len()-2)) {
                        if j == 0 && inclusive_left == &false {
                            // leftmost bin must be left inclusive even if overall strategy is to be right inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if j == (sorted_edges.len()-2) && inclusive_left == &true {
                            // rightmost bin must be right inclusive even if overall strategy is to be left inclusive
                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        } else if inclusive_left == &true {
                            bin_vec.push(format!("[{}, {})", sorted_edges[j], sorted_edges[j+1]));
                        } else {
                            bin_vec.push(format!("({}, {}]", sorted_edges[j], sorted_edges[j+1]));
                        }
                        break;
                }
            }
        }
    }
    // convert bin vector to Array and return
    let bin_array: Array1<String> = Array1::from(bin_vec);
    return bin_array.into_dyn();
}

/// clamps data to [min, max]
///
/// # Arguments
/// * `data` - data you want to clamp
/// * `min` - lower bound on data
/// * `max` - upper bound on data
///
/// # Return
/// array of clamped data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::transformations::clamp;
///
/// let data: ArrayD<f64> = arr1(&[1., -2., 3., 5.]).into_dyn();
/// let min: f64 = 0.;
/// let max: f64 = 4.;
/// let clamped: ArrayD<f64> = clamp(&data, &min, &max);
/// println!("{:?}", clamped);
/// ```
pub fn clamp(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {

    let mut data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    for i in 0..data_vec.len() {
        if data_vec[i] < *min {
            data_vec[i] = *min;
        } else if data_vec[i] > *max {
            data_vec[i] = *max;
        }
    }
    return arr1(&data_vec).into_dyn();
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
/// let imputed: ArrayD<f64> = impute_float_uniform(&data, &min, &max);
/// println!("{:?}", imputed);
/// ```

pub fn impute_float_uniform(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {

    let mut data_vec: Vec<f64> = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if data[i].is_nan() {
            data_vec.push(noise::sample_uniform(*min, *max));
        } else {
            data_vec.push(data[i]);
        }
    }
    return arr1(&data_vec).into_dyn();
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
/// let imputed: ArrayD<f64> = impute_float_gaussian(&data, &shift, &scale, &min, &max);
/// println!("{:?}", imputed);
/// ```
pub fn impute_float_gaussian(data: &ArrayD<f64>, shift: &f64, scale: &f64, min: &f64, max: &f64) -> ArrayD<f64> {

    let mut data_vec: Vec<f64> = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if data[i].is_nan() {
            data_vec.push(noise::sample_gaussian_truncated(*shift, *scale, *min, *max));
        } else {
            data_vec.push(data[i]);
        }
    }
    return arr1(&data_vec).into_dyn();
}

/// Given data and min/max values, returns data with imputed values in place of NaN.
/// For now, imputed values are generated uniformly at random between the min and max values provided,
/// we may later add the ability to impute according to other distributions
///
/// NOTE: This function imputes integer values, although the input and output arrays are
///       made up of floats. integer types in rust do not support NAN, so if we have missing data,
///       it needs to be represented as a float
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
/// use core::f64::NAN;
/// use yarrow_runtime::utilities::transformations::impute_int_uniform;
/// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
/// let min: f64 = 0.0;
/// let max: f64 = 10.0;
/// let imputed: ArrayD<f64> = impute_int_uniform(&data, &min, &max);
/// println!("{:?}", imputed);
/// ```
pub fn impute_int_uniform(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {

    // ensure that min/max are integers -- they are passed as floats for consistency with our more general imputation architecture
    assert!(min.fract() == 0.0 && max.fract() == 0.0);

    let mut data_vec: Vec<f64> = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if data[i].is_nan() {
            data_vec.push(noise::sample_uniform_int(&(*min as i64), &(*max as i64)) as f64);
        } else {
            data_vec.push(data[i]);
        }
    }
    return arr1(&data_vec).into_dyn();
}

pub struct ImputationParameters {
    n: i64,
    seed: Option<[u8; 32]>,
    distribution: ArrayD<String>,
    data_type: ArrayD<String>,
    min: ArrayD<f64>,
    max: ArrayD<f64>,
    shift: ArrayD<Option<f64>>,
    scale: ArrayD<Option<f64>>
}

pub fn clamp_and_impute(data: &ArrayD<f64>, params: &ImputationParameters) -> ArrayD<f64> {
    // enforce that data are vector or matrix
    // NOTE: may not want/need this eventually
    assert!(data.ndim() <= 2);

    // set string literals for fields in ImputationParameters struct that are of type String
    let Uniform: String = "Uniform".to_string();
    let Gaussian: String = "Gaussian".to_string();
    let Float: String = "Float".to_string();
    let Int: String = "Int".to_string();

    // get parameter array lengths
    // NOTE: this needs to be kept up to date to reflect every field in the ImputationParameters struct
    let distribution_len: i64 = params.distribution.len() as i64;
    let data_type_len: i64 = params.data_type.len() as i64;
    let min_len: i64 = params.min.len() as i64;
    let max_len: i64 = params.max.len() as i64;
    let shift_len: i64 = params.shift.len() as i64;
    let scale_len: i64 = params.scale.len() as i64;

    // find correct length for each parameter array based on dimensionality of data
    let correct_param_length: i64 = match data.ndim() {
        0 => 1, // datum is a single constant
        1 => 1, // data are a single vector
        2 => data.len_of(Axis(0)) as i64, // data are k vectors, this finds k
        _ => panic!("dimension of input data not supported")
    };

    // ensure that parameters are of correct length
    assert!(correct_param_length == distribution_len &&
            correct_param_length == data_type_len &&
            correct_param_length == min_len &&
            correct_param_length == max_len &&
            correct_param_length == shift_len &&
            correct_param_length == scale_len);

    // get actual number of observations in data
    let real_n: i64 = match data.ndim() {
        0 => 1,
        1 => data.len_of(Axis(0)) as i64,
        2 => data.len_of(Axis(1)) as i64,
        _ => panic!("dimension of input data not supported")
    };

    // initialize new data -- this is what we ultimately return from the function
    let mut new_data: ArrayD<f64>= match data.ndim() {
        0 => arr0(0.).into_dyn(),
        1 => Array1::<f64>::zeros(real_n as usize).into_dyn(),
        2 => Array2::<f64>::zeros((data.len_of(Axis(0)),real_n as usize)).into_dyn(),
        _ => panic!("dimension of input data not supported")
    };

    // initialize all data steps -- we create all of them in order to enforce roughly equal timing
    // regardless of whether or not n == real_n
    let mut imputed_data: ArrayD<f64>;
    let mut imputed_clamped_data: ArrayD<f64>;
    let mut subsampled_imputed_clamped_data: ArrayD<f64>;
    let mut augmented_imputed_clamped_data: ArrayD<f64>;

    // for each column in data:
    for i in 0..correct_param_length {
        // do standard data imputation
        imputed_data = match params {
            ImputationParameters { distribution: Uniform, data_type: Float, .. } => impute_float_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]),
            ImputationParameters { distribution: Uniform, data_type: Int, .. } => impute_int_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]),
            ImputationParameters { distribution: Gaussian, data_type: Float, .. } => impute_float_gaussian(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.shift[i as usize].unwrap(), &params.scale[i as usize].unwrap(), &params.min[i as usize], &params.max[i as usize]),
            _ => panic!("distribution/data_type combination not supported")
        };

        // clamp data to bounds
        imputed_clamped_data = clamp(&(imputed_data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]);

        // create subsampled version of data (returned if n < real_n)
        let k: i64 = cmp::min(params.n, real_n);
        let probabilities: ArrayD<f64> = arr1(&vec![1./(k as f64)]).into_dyn();
        subsampled_imputed_clamped_data = aggregations::create_subset(&imputed_clamped_data, &probabilities, &k, &params.seed);

        // create augmented version of data (returned if n > real_n)
        let mut augmentation_data: ArrayD<f64> = arr1(&vec![NAN; cmp::max(0, params.n - real_n) as usize]).into_dyn();
        augmentation_data = match params {
            ImputationParameters { distribution: Uniform, data_type: Float, .. } => impute_float_uniform(&augmentation_data, &params.min[i as usize], &params.max[i as usize]),
            ImputationParameters { distribution: Uniform, data_type: Int, .. } => impute_int_uniform(&augmentation_data, &params.min[i as usize], &params.max[i as usize]),
            ImputationParameters { distribution: Gaussian, data_type: Float, .. } => impute_float_gaussian(&augmentation_data, &params.shift[i as usize].unwrap(), &params.scale[i as usize].unwrap(), &params.min[i as usize], &params.max[i as usize]),
            _ => panic!("distribution/data_type combination not supported")
        };
        let augmentation_vec: Vec<f64> = augmentation_data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
        augmented_imputed_clamped_data = stack![Axis(0), imputed_clamped_data.slice(s![0, ..]), augmentation_vec].to_owned().into_dyn();

        // create data
        if params.n == real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&imputed_clamped_data);
        } else if params.n < real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&subsampled_imputed_clamped_data);
        } else if params.n > real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&augmented_imputed_clamped_data);
        }
    }
    return new_data;
}
