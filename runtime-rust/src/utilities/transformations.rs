use std::string::String;
use std::vec::Vec;
use std::cmp;
use ndarray::prelude::*;
use ndarray::{stack, Zip};



use crate::utilities::noise;
use crate::utilities::aggregations;
use crate::utilities::utilities;

// TODO: this is temporary function for testing purposes
pub fn convert_to_matrix<T>(data: &ArrayD<T>) -> ArrayD<T> where T: Clone {
    match data.ndim() {
        0 => data.clone().insert_axis(Axis(0)).clone().insert_axis(Axis(0)),
        1 => data.clone().insert_axis(Axis(0)),
        2 => data.clone(),
        _ => panic!("unsupported dimension")
    }
}

pub fn convert_from_matrix<T>(data: &ArrayD<T>, original_dim: &u8) -> ArrayD<T> where T: Clone {
    match original_dim {
        0 => data.clone().remove_axis(Axis(0)).clone().remove_axis(Axis(0)),
        1 => data.clone().remove_axis(Axis(0)),
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
    let original_dim: u8 = data.ndim() as u8;
    let new_data: ArrayD<f64> = convert_to_matrix(data);
    let mut new_bin_array: ArrayD<String> = Array::default(new_data.shape());

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
        let bin_array: ArrayD<String> = arr1(&bin_vec).into_dyn();
        new_bin_array.slice_mut(s![k as usize, ..]).assign(&bin_array);
    }
    return convert_from_matrix(&new_bin_array, &original_dim);;
}

pub fn count<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>) -> Vec<Option<Vec<i64>>> where T: Clone, T: PartialEq {
    /// Gets count of data elements for each category
    ///
    /// Example
    /// ```
    /// let data: ArrayD<i64> = arr2(&[ [1,1,1,1,1,2,2,2,2,3,3,3,4,4,5],
    ///                                 [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5] ]).into_dyn();
    /// let categories: Vec<Vec<i64>> = vec![vec![1,3,5], vec![2,4]];
    /// let t: Vec<Vec<i64>> = count(&data, &categories);
    /// println!("{:?}", t);
    /// ```

    let data_2d: ArrayD<T> = convert_to_matrix(data);
    let mut counts: Vec<Option<Vec<i64>>> = Vec::with_capacity(categories.len());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;

    for i in 0..n_cols {
        let data_vec: Vec<T> = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
                           into_dimensionality::<Ix1>().unwrap().to_vec();
        let category_vec: Vec<T> = categories[i as usize].clone().unwrap();
        let mut counts_vec: Vec<i64> = vec![0; category_vec.len()];

        for j in 0..data_vec.len() {
            for k in 0..category_vec.len() {
                if data_vec[j as usize] == category_vec[k as usize] {
                    counts_vec[k] += 1;
                }
            }
        }

        counts.push(Some(counts_vec));
    }

    return counts;
}


pub fn broadcast_map<T>(
    left: &ArrayD<T>,
    right: &ArrayD<T>,
    operator: &dyn Fn(&T, &T) -> T ) -> Result<ArrayD<T>, String> where T: std::clone::Clone, T: Default, T: Copy {
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

            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros)
                .and(left)
                .and(right).apply(|acc, &l, &r| *acc = operator(&l, &r));
            Ok(zeros)
        },
        (l, r) if l == 1 && r == 0 => {
            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros).and(left).apply(|acc, &l| *acc = operator(&l, &right.first().unwrap()));
            Ok(zeros)
        },
        (l, r) if l == 0 && r == 1 => {
            let mut zeros: ArrayD<T> = Array::default(left.shape());
            Zip::from(&mut zeros).and(right).apply(|acc, &r| *acc = operator(&left.first().unwrap(), &r));
            Ok(zeros)
        },
        _ => Err("unsupported shapes for left and right vector in broadcast_map".to_string())
    }
}

pub fn clamp_numeric<T>(data: &ArrayD<T>, min: &ArrayD<T>, max: &ArrayD<T>)
    -> ArrayD<T> where T: PartialOrd, T: Clone, T: Default {
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

    let data_2d: ArrayD<T> = convert_to_matrix(data);
    let mut clamped_data: ArrayD<T> = Array::default(data_2d.shape());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;

    for i in 0..n_cols {
        let mut data_vec = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
                           into_dimensionality::<Ix1>().unwrap().to_vec();
        for j in 0..data_vec.len() {
                if data_vec[j] < min[i as usize] {
                    data_vec[j] = min[i as usize].clone();
                } else if data_vec[j] > max[i as usize] {
                    data_vec[j] = max[i as usize].clone();
                }
        }
        clamped_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
    }
    return clamped_data;
}

pub fn clamp_categorical<T>(data: &ArrayD<T>, categories: &Vec::<Vec<T>>, null_value: &ArrayD<T>) -> ArrayD<T> where T:Clone, T:PartialEq, T:Default {
    let original_dim: u8 = data.ndim() as u8;
    let data_2d: ArrayD<T> = convert_to_matrix(data);
    let mut clamped_data: ArrayD<T> = Array::default(data_2d.shape());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;
    let mut category_vec: Vec<T>;
    let mut n_categories: i64;

    for i in 0..n_cols {
        category_vec = categories[i as usize].clone();
        n_categories = category_vec.len() as i64;
        let mut data_vec = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
                          into_dimensionality::<Ix1>().unwrap().to_vec();
        for j in 0..data_vec.len() {
                if !category_vec.contains(&data_vec[j]) {
                    data_vec[j] = null_value[i as usize].clone();
                }
        }
        clamped_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
    }
    return convert_from_matrix(&clamped_data, &original_dim);
}

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

pub fn impute_numeric(data: &ArrayD<f64>, distribution: &String,
                      min: &ArrayD<f64>, max: &ArrayD<f64>,
                      shift: &Option<ArrayD<f64>>, scale: &Option<ArrayD<f64>>) -> ArrayD<f64> {
    // set string literals for arguments that are of type String
    let _Uniform: String = "Uniform".to_string(); // Distributions
    let _Gaussian: String = "Gaussian".to_string();
    // let Float: String = "Float".to_string(); // Data Types
    // let Int: String = "Int".to_string();

    // initialize new data
    let original_dim: u8 = data.ndim() as u8;
    let data_2d: ArrayD<f64> = convert_to_matrix(data);
    let mut imputed_data: ArrayD<f64> = Array::default(data_2d.shape());
    let n_cols: i64 = data.len_of(Axis(0)) as i64;

    // for each column in data:
    let mut imputed_col: ArrayD<f64>;
    for i in 0..n_cols {
        let (shift_i, scale_i): (Option<ArrayD<f64>>, Option<ArrayD<f64>>) = match distribution {
            _Gaussian => (Some(arr1(&[shift.as_ref().unwrap()[i as usize]]).into_dyn()),
                         Some(arr1(&[scale.as_ref().unwrap()[i as usize]]).into_dyn())),
            _Uniform => (None, None),
            _ => panic!("distribution not supported".to_string())
        };
        // do standard data imputation
        imputed_col = match distribution.to_string() {
            _Uniform => impute_float_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(),
                                                     &(min[i as usize]), &(max[i as usize])),
            // (Uniform, Int) => impute_int_uniform(&(data.slice(s![0, ..])).to_owned().into_dyn(), &min[i as usize], &max[i as usize]),
            _Gaussian => impute_float_gaussian(&(data.slice(s![0, ..])).to_owned().into_dyn(), &shift_i.unwrap().first().unwrap(),
                                                                                                       &scale_i.unwrap().first().unwrap(),                                                        &min[i as usize], &max[i as usize]),
            _ => panic!("distribution/data_type combination not supported")
        };
        imputed_data.slice_mut(s![i as usize, ..]).assign(&imputed_col);
    }
    return convert_from_matrix(&imputed_data, &original_dim);
}

pub fn impute_categorical<T>(data: &ArrayD<T>, categories: &Vec::<Vec<T>>, probabilities: &Vec::<Vec<f64>>, null_value: &ArrayD<T>) ->
                             ArrayD<T> where T:Clone, T:PartialEq, T:Default {
    let original_dim: u8 = data.ndim() as u8;
    let data_2d: ArrayD<T> = convert_to_matrix(data);
    let mut imputed_data: ArrayD<T> = Array::default(data_2d.shape());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;
    let mut category_vec: Vec<T>;
    let mut probability_vec: Vec<f64>;
    let mut n_categories: i64;

    for i in 0..n_cols {
        category_vec = categories[i as usize].clone();
        probability_vec = probabilities[i as usize].clone();
        n_categories = category_vec.len() as i64;
        let mut data_vec = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
                          into_dimensionality::<Ix1>().unwrap().to_vec();
        for j in 0..data_vec.len() {
                if data_vec[j] == null_value[j] {
                    data_vec[j] = utilities::sample_from_set(&category_vec, &probability_vec);
                }
        }
        imputed_data.slice_mut(s![i as usize, ..]).assign(&arr1(&data_vec).into_dyn());
    }
    return convert_from_matrix(&imputed_data, &original_dim);
}

pub fn resize_numeric(data: &ArrayD<f64>, n: &u64, distribution: &String,
                      min: &ArrayD<f64>, max: &ArrayD<f64>,
                      shift: &Option<ArrayD<f64>>, scale: &Option<ArrayD<f64>>) -> ArrayD<f64> {
    // set string literals for arguments that are of type String
    let _Uniform: String = "Uniform".to_string(); // Distributions
    let _Gaussian: String = "Gaussian".to_string();
    // let Float: String = "Float".to_string(); // Data Types
    // let Int: String = "Int".to_string();

    // get number of observations in actual data
    let real_n: u64 = data.len_of(Axis(1)) as u64;

    // initialize new data
    let _original_dim: u8 = data.ndim() as u8;
    let mut data_2d = convert_to_matrix(data);
    let n_cols: i64 = data.len_of(Axis(0)) as i64;
    let mut new_data: ArrayD<f64> = Array::default( (data.len_of(Axis(0)), real_n as usize) ).into_dyn();

    // initialize columns for resizing step
    let mut column: ArrayD<f64>;
    let mut subsampled_column: ArrayD<f64>;
    let mut _augmented_column: ArrayD<f64>;

    // for each column in data:
    for i in 0..n_cols {
        // get column
        column = data_2d.slice_mut(s![i as usize, ..]).to_owned().into_dyn();

        // create subsampled version of data (returned if n < real_n)
        let k: u64 = cmp::min(*n, real_n);
        let sampling_probabilities: ArrayD<f64> = arr1(&vec![1./(k as f64)]).into_dyn();
        subsampled_column = aggregations::create_subset(&column, &sampling_probabilities, &(k as u64));

        // create augmented version of data (returned if n > real_n)
        let (shift_i, scale_i): (Option<ArrayD<f64>>, Option<ArrayD<f64>>) = match distribution {
            _Gaussian => match (shift, scale) {
                (Some(shift), Some(scale)) =>
                    (Some(arr1(&[shift[i as usize]]).into_dyn()), Some(arr1(&[scale[i as usize]]).into_dyn())),
                _ => panic!("gaussian distribution requires both shift and scale to be defined".to_string())
            },
            _Uniform => (None, None),
            _ => panic!("distribution not supported".to_string())
        };

        let augmentation_data = impute_numeric(&column, distribution,
                                                    &arr1(&[min[i as usize]]).into_dyn(),
                                                    &arr1(&[max[i as usize]]).into_dyn(),
                                                    &shift_i,
                                                    &scale_i,
                                                    );
        let augmentation_vec = augmentation_data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
        let augmented_column = stack![Axis(0), column.slice(s![0, ..]), augmentation_vec].clone().into_dyn();

        // create data
        if n == &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&column);
        } else if n < &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&subsampled_column);
        } else if n > &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&augmented_column);
        }
    }
    return new_data;
}

pub fn resize_categorical<T>(data: &ArrayD<T>, n: &u64,
                             categories: &Vec<Vec<T>>, probabilities: &Vec<Vec<f64>>, null_value: &ArrayD<T>,)
                                -> ArrayD<T> where T: Clone, T: Copy, T: PartialEq, T: Default {
    // set string literals for arguments that are of type String
    let _Uniform: String = "Uniform".to_string(); // Distributions
    let _Gaussian: String = "Gaussian".to_string();
    let _Float: String = "Float".to_string(); // Data Types
    let _Int: String = "Int".to_string();

    // get number of observations in actual data
    let real_n: u64 = data.len_of(Axis(1)) as u64;

    // initialize new data
    let _original_dim: u8 = data.ndim() as u8;
    let mut data_2d = convert_to_matrix(data);
    let n_cols: i64 = data.len_of(Axis(0)) as i64;
    let mut new_data: ArrayD<T> = Array::default( (data.len_of(Axis(0)), real_n as usize) ).into_dyn();

    // for each column in data:
    for i in 0..n_cols {
        // get column
        let column = data_2d.slice_mut(s![i as usize, ..]).to_owned().into_dyn();

        // create subsampled version of data (returned if n < real_n)
        let k: u64 = cmp::min(*n, real_n);
        let sampling_probabilities: ArrayD<f64> = arr1(&vec![1./(k as f64)]).into_dyn();
        let subsampled_column = aggregations::create_subset(&column, &sampling_probabilities, &(k as u64));

        // create augmented version of data (returned if n > real_n)
        let augmentation_data = impute_categorical(&column, &vec![categories[i as usize].clone()],
                                                   &vec![probabilities[i as usize].clone()],
                                                   &arr1(&[null_value[i as usize].clone()]).into_dyn()
                                    );
        let augmentation_vec = augmentation_data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
        let augmented_column = stack![Axis(0), column.slice(s![0, ..]), augmentation_vec].clone().into_dyn();

        // create data
        if n == &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&column);
        } else if n < &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&subsampled_column);
        } else if n > &real_n {
            new_data.slice_mut(s![i as usize, ..]).assign(&augmented_column);
        }
    }
    return new_data;
}

