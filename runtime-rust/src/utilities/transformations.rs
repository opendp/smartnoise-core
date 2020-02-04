use std::string::String;
use std::vec::Vec;
use ndarray::prelude::*;
use crate::utilities::noise;
use core::f64::NAN;

pub fn bin(data: &ArrayD<f64>, edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {
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

pub fn impute_f64(data: &ArrayD<f64>, min: &f64, max: &f64) -> ArrayD<f64> {
    /// Given data and min/max values, returns data with imputed values in place of NaN.
    /// For now, imputed values are generated uniformly at random between the min and max values provided,
    /// we may later add the ability to impute according to other distributions
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

pub fn impute_i64(data: &ArrayD<f64>, min: &i64, max: &i64) -> ArrayD<f64> {
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

    let mut data_vec: Vec<f64> = Vec::with_capacity(data.len());
        for i in 0..data.len() {
        if data[i].is_nan() {
            data_vec.push(noise::sample_uniform_int(min, max) as f64);
        } else {
            data_vec.push(data[i]);
        }
    }
    return arr1(&data_vec).into_dyn();
}