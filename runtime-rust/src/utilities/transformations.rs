use std::string::String;
use std::vec::Vec;
use std::cmp;
use ndarray::prelude::*;
use ndarray::{stack, Zip};
use core::f64::NAN;
use num;

use crate::utilities::noise;
use crate::utilities::aggregations;

// TODO: this is temporary function for testing purposes
pub fn convert_to_matrix<T>(data: &ArrayD<T>) -> ArrayD<T> where T: Clone {
    match data.ndim() {
        0 => data.clone().insert_axis(Axis(0)).clone().insert_axis(Axis(0)),
        1 => data.clone().insert_axis(Axis(0)),
        2 => data.clone(),
        _ => panic!("unsupported dimension")
    }
}

pub fn bin(data: &ArrayD<f64>, edges: &ArrayD<f64>, inclusive_left: &ArrayD<bool>) -> ArrayD<String> {
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
    /// // set up data
    /// let data: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5., 12., 19., 24., 90., 98.]).into_dyn();
    /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
    /// let inclusive_left: bool = arr1(&[false]).into_dyn();
    ///
    /// // bin data
    /// let binned_data: ArrayD<String> = bin(&data, &edges, &inclusive_left);
    /// println!("{:?}", binned_data);
    /// ```

    // initialize new data -- this is what we ultimately return from the function
    let mut new_data: ArrayD<f64> = convert_to_matrix(data);
    let mut new_bin_array: ArrayD<String> = Array::default(data.shape());

    let n_cols: i64 = data.len_of(Axis(0)) as i64;

    for k in 0..n_cols {
        // create vector versions of data and edges
        let data_vec: Vec<f64> = data.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>().unwrap().to_vec();
        let mut sorted_edges: Vec<f64> = edges.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>().unwrap().to_vec();

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
                        (data_vec[i] == sorted_edges[j+1] && inclusive_left[k as usize] == false) ||
                        // element is equal to the right bin edge and we are checking our rightmost bin
                        (data_vec[i] == sorted_edges[j+1] && j == (sorted_edges.len()-2)) {
                            if j == 0 && inclusive_left[k as usize] == false {
                                // leftmost bin must be left inclusive even if overall strategy is to be right inclusive
                                bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                            } else if j == (sorted_edges.len()-2) && inclusive_left[k as usize] == true {
                                // rightmost bin must be right inclusive even if overall strategy is to be left inclusive
                                bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
                            } else if inclusive_left[k as usize] == true {
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
        let mut bin_array: ArrayD<String> = arr1(&bin_vec).into_dyn();
        new_bin_array.slice_mut(s![k as usize, ..]).assign(&bin_array);
    }
    return new_bin_array;
}

pub fn broadcast_map<T>(
    left: &ArrayD<T>,
    right: &ArrayD<T>,
    operator: &dyn Fn(&T, &T) -> T ) -> Result<ArrayD<T>, String> where T: std::clone::Clone, T: num::Zero, T: Copy {
    /// Broadcast left and right to match each other, and map an operator over the pairs
    ///
    /// # Arguments
    /// * `left` - left vector to map over
    /// * `right` - right vector to map over
    /// * `operator` - function to apply to each pair
    ///
    /// # Return
    /// An array of mapped data
    ///
    /// # Example
    /// ```
    /// let left: Array1<f64> = arr1!([1., -2., 3., 5.]);
    /// let right: Array1<f64> = arr1!([2.]);
    /// let mapped: Array1<f64> = broadcast_map(&left, &right, &|l, r| l.max(r));
    /// println!("{:?}", mapped); // [2., 2., 3., 5.]
    /// ```

    match (left.ndim(), right.ndim()) {
        (l, r) if l == 0 && r == 0 =>
            Ok(Array::from_shape_vec(vec![],
                                  vec![operator(left.first().unwrap(), right.first().unwrap())]).unwrap()),
        (l, r) if l == 1 && r == 1 => {
            if left.len() != right.len() {
                return Err("the size of the left and right vectors do not match".to_string())
            }

            let mut zeros: ArrayD<T> = Array::zeros(left.shape());
            Zip::from(&mut zeros)
                .and(left)
                .and(right).apply(|acc, &l, &r| *acc = operator(&l, &r));
            Ok(zeros)
        },
        (l, r) if l == 1 && r == 0 => {
            let mut zeros: ArrayD<T> = Array::zeros(left.shape());
            Zip::from(&mut zeros).and(left).apply(|acc, &l| *acc = operator(&l, &right.first().unwrap()));
            Ok(zeros)
        },
        (l, r) if l == 0 && r == 1 => {
            let mut zeros: ArrayD<T> = Array::zeros(left.shape());
            Zip::from(&mut zeros).and(right).apply(|acc, &r| *acc = operator(&left.first().unwrap(), &r));
            Ok(zeros)
        },
        _ => Err("unsupported shapes for left and right vector in broadcast_map".to_string())
    }
}

pub fn clamp_numeric(data: &ArrayD<f64>, min: &ArrayD<f64>, max: &ArrayD<f64>) -> ArrayD<f64> {
    /// Clamps each column of numeric data to [min, max]
    ///
    /// # Example
    /// ```
    /// let data = arr2(&[ [1.,2.,3.], [7.,11.,9.] ]).into_dyn();
    /// let mut data_2d: ArrayD<f64> = convert_to_matrix(&data);
    /// let mins: ArrayD<f64> = arr1(&[0.5,8.]).into_dyn();
    /// let maxes: ArrayD<f64> = arr1(&[2.5,10.]).into_dyn();
    /// let mut clamped_data = clamp_numeric(&data_2d, &mins, &maxes);
    /// println!("{:?}", data_2d);
    /// println!("{:?}", clamped_data);
    /// ```
    let mut data_2d: ArrayD<f64> = convert_to_matrix(data);
    let mut clamped_data: ArrayD<f64> = Array::default(data_2d.shape());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;

    for i in 0..n_cols {
        let mut data_vec = data_2d.slice(s![i as usize, ..]).to_owned().into_dyn().clone().
                           into_dimensionality::<Ix1>().unwrap().to_vec();
        for j in 0..data_vec.len() {
                if data_vec[j] < min[i as usize] {
                    data_vec[j] = min[i as usize];
                } else if data_vec[j] > max[i as usize] {
                    data_vec[j] = max[i as usize];
                }
        }
        clamped_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
    }
    return clamped_data;
}

pub fn clamp_categorical(data: &ArrayD<String>, categories: &ArrayD<String>, null_value: &String) -> ArrayD<String> {
    let mut data_2d: ArrayD<String> = convert_to_matrix(data);
    let mut clamped_data: ArrayD<String> = Array::default(data_2d.shape());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;
    let category_vec: Vec<String> = categories.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let n_categories: i64 = category_vec.len() as i64;

    for i in 0..n_cols {
        let mut data_vec = data_2d.slice(s![i as usize, ..]).to_owned().into_dyn().clone().
                           into_dimensionality::<Ix1>().unwrap().to_vec();
        for j in 0..data_vec.len() {
                if !category_vec.contains(&data_vec[j]) {
                    // sample uni
                    data_vec[j] = null_value.to_string();
                }
        }
        clamped_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
    }
    return clamped_data;
}

// pub fn clamp<T>(data: &ArrayD<T>, min: &Option(ArrayD<f64>),
//              max: &Option(ArrayD<f64>), categories: &Option(ArrayD<String>)
//              ) -> ArrayD<T> where T: Copy {

//     let mut data_2d = convert_to_matrix(data);
//     let mut clamped_data = Array::default(data_2d.shape());

//     let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;

//     for i in 0..n_cols {
//         let mut data_vec = data_2d.slice(s![i as usize, ..]).to_owned().into_dyn().clone().
//                            into_dimensionality::<Ix1>().unwrap().to_vec();
//         for j in 0..data_vec.len() {
//             if min.is_some() && max.is_some() {
//                 if data_vec[j] < min.unwrap()[i as usize] {
//                     data_vec[j] = min.unwrap()[i as usize];
//                 } else if data_vec[j] > max.unwrap()[i as usize] {
//                     data_vec[j] = max.unwrap()[i as usize];
//                 }
//             } else if categories.is_some() {
//                 // TODO: write what to do if elem not in categories
//                 panic!("either min/max or categories must be set");
//             } else {
//                 panic!("either min/max or categories must be set");
//             }
//         }
//         clamped_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
//     }
//     return clamped_data;
// }

pub fn impute_float_uniform(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {
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
    /// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
    /// let min: f64 = 0.;
    /// let max: f64 = 10.;
    /// let imputed: ArrayD<f64> = impute(&data, &min, &max);
    /// println!("{:?}", imputed);
    /// ```

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

pub fn impute_float_gaussian(data: &ArrayD<f64>, shift: &f64, scale: &f64, min: &f64, max: &f64) -> ArrayD<f64> {
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
    /// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
    /// let shift: f64 = 5;
    /// let scale: f64 = 7;
    /// let min: f64 = 0.;
    /// let max: f64 = 10.;
    /// let imputed: ArrayD<f64> = impute(&data, &shift, &scale, &min, &max);
    /// println!("{:?}", imputed);
    /// ```

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

pub fn impute_int_uniform(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {
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
    /// let data: ArrayD<f64> = arr1(&[1., NAN, 3., NAN]).into_dyn();
    /// let min: i64 = 0;
    /// let max: i64 = 10;
    /// let imputed: ArrayD<f64> = impute(&data, &min, &max);
    /// println!("{:?}", imputed);
    /// ```

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

// pub struct ImputationParameters {
//     n: i64,
//     distribution: ArrayD<String>,
//     data_type: ArrayD<String>,
//     min: ArrayD<f64>,
//     max: ArrayD<f64>,
//     shift: ArrayD<Option<f64>>,
//     scale: ArrayD<Option<f64>>
// }

// pub fn clamp_and_impute(data: &ArrayD<f64>, params: &ImputationParameters) -> ArrayD<f64> {
//     // enforce that data are vector or matrix
//     // NOTE: may not want/need this eventually
//     assert!(data.ndim() <= 2);

//     // set string literals for fields in ImputationParameters struct that are of type String
//     let Uniform: String = "Uniform".to_string();
//     let Gaussian: String = "Gaussian".to_string();
//     let Float: String = "Float".to_string();
//     let Int: String = "Int".to_string();

//     // get parameter array lengths
//     // NOTE: this needs to be kept up to date to reflect every field in the ImputationParameters struct
//     let distribution_len: i64 = params.distribution.len() as i64;
//     let data_type_len: i64 = params.data_type.len() as i64;
//     let min_len: i64 = params.min.len() as i64;
//     let max_len: i64 = params.max.len() as i64;
//     let shift_len: i64 = params.shift.len() as i64;
//     let scale_len: i64 = params.scale.len() as i64;

//     // find correct length for each parameter array based on dimensionality of data
//     let correct_param_length: i64 = match data.ndim() {
//         0 => 1, // datum is a single constant
//         1 => 1, // data are a single vector
//         2 => data.len_of(Axis(0)) as i64, // data are k vectors, this finds k
//         _ => panic!("dimension of input data not supported")
//     };

//     // ensure that parameters are of correct length
//     assert!(correct_param_length == distribution_len &&
//             correct_param_length == data_type_len &&
//             correct_param_length == min_len &&
//             correct_param_length == max_len &&
//             correct_param_length == shift_len &&
//             correct_param_length == scale_len);

//     // get actual number of observations in data
//     let real_n: i64 = match data.ndim() {
//         0 => 1,
//         1 => data.len_of(Axis(0)) as i64,
//         2 => data.len_of(Axis(1)) as i64,
//         _ => panic!("dimension of input data not supported")
//     };

//     // initialize new data -- this is what we ultimately return from the function
//     let mut new_data: ArrayD<f64>= match data.ndim() {
//         0 => arr0(0.).into_dyn(),
//         1 => Array1::<f64>::zeros(real_n as usize).into_dyn(),
//         2 => Array2::<f64>::zeros((data.len_of(Axis(0)),real_n as usize)).into_dyn(),
//         _ => panic!("dimension of input data not supported")
//     };

//     // initialize all data steps -- we create all of them in order to enforce roughly equal timing
//     // regardless of whether or not n == real_n
//     let mut imputed_data: ArrayD<f64>;
//     let mut imputed_clamped_data: ArrayD<f64>;
//     let mut subsampled_imputed_clamped_data: ArrayD<f64>;
//     let mut augmented_imputed_clamped_data: ArrayD<f64>;

//     // for each column in data:
//     for i in 0..correct_param_length {
//         // do standard data imputation
//         imputed_data = match params {
//             ImputationParameters { distribution: Uniform, data_type: Float, .. } => impute_float_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]),
//             ImputationParameters { distribution: Uniform, data_type: Int, .. } => impute_int_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]),
//             ImputationParameters { distribution: Gaussian, data_type: Float, .. } => impute_float_gaussian(&(data.slice(s![0, ..])).to_owned().into_dyn(), &params.shift[i as usize].unwrap(), &params.scale[i as usize].unwrap(), &params.min[i as usize], &params.max[i as usize]),
//             _ => panic!("distribution/data_type combination not supported")
//         };

//         // clamp data to bounds
//         imputed_clamped_data = clamp(&(imputed_data.slice(s![0, ..])).to_owned().into_dyn(), &params.min[i as usize], &params.max[i as usize]);

//         // create subsampled version of data (returned if n < real_n)
//         let k: i64 = cmp::min(params.n, real_n);
//         let probabilities: ArrayD<f64> = arr1(&vec![1./(k as f64)]).into_dyn();
//         subsampled_imputed_clamped_data = aggregations::create_subset(&imputed_clamped_data, &probabilities, &k);

//         // create augmented version of data (returned if n > real_n)
//         let mut augmentation_data: ArrayD<f64> = arr1(&vec![NAN; cmp::max(0, params.n - real_n) as usize]).into_dyn();
//         augmentation_data = match params {
//             ImputationParameters { distribution: Uniform, data_type: Float, .. } => impute_float_uniform(&augmentation_data, &params.min[i as usize], &params.max[i as usize]),
//             ImputationParameters { distribution: Uniform, data_type: Int, .. } => impute_int_uniform(&augmentation_data, &params.min[i as usize], &params.max[i as usize]),
//             ImputationParameters { distribution: Gaussian, data_type: Float, .. } => impute_float_gaussian(&augmentation_data, &params.shift[i as usize].unwrap(), &params.scale[i as usize].unwrap(), &params.min[i as usize], &params.max[i as usize]),
//             _ => panic!("distribution/data_type combination not supported")
//         };
//         let augmentation_vec: Vec<f64> = augmentation_data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
//         augmented_imputed_clamped_data = stack![Axis(0), imputed_clamped_data.slice(s![0, ..]), augmentation_vec].to_owned().into_dyn();

//         // create data
//         if params.n == real_n {
//             new_data.slice_mut(s![i as usize, ..]).assign(&imputed_clamped_data);
//         } else if params.n < real_n {
//             new_data.slice_mut(s![i as usize, ..]).assign(&subsampled_imputed_clamped_data);
//         } else if params.n > real_n {
//             new_data.slice_mut(s![i as usize, ..]).assign(&augmented_imputed_clamped_data);
//         }
//     }
//     return new_data;
// }
