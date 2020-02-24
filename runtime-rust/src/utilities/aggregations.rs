use ndarray::prelude::*;
use std::collections::HashMap;

use crate::utilities::transformations;
use crate::utilities::noise;

// pub fn count< T:PartialEq >(data: &ArrayD<T>, group_by: &Option<ArrayD<T>>) -> ArrayD<f64> {
//     // Accepts data and an optional array of values to be counted, and returns counts of each value.
//     // If no values are provided, the function returns the overall count of the entire data.
//     //
//     // # Arguments
//     // * `data` - Array of data for which you want counts. Data type can be any that supports the `PartialEq` trait.
//     // * `group_by` (Optional) - Array of values for which you want counts. Data type should be the same as `data`.
//     //
//     // # Return
//     // Array of counts
//     //
//     // # Example
//     // ```
//     // //////////////////
//     // // numeric data //
//     // //////////////////
//     // let data: ArrayD<f64> = arr1(&[1., 1., 2., 3., 4., 4., 4.]).into_dyn();
//     // let group_by: ArrayD<f64> = arr1(&[1., 2., 4.]).into_dyn();
//     //
//     // // count specific values
//     // let count_1: ArrayD<f64> = count(&data, &Some(group_by));
//     // println!("{:?}", count_1);
//     // // get overall size of data
//     // let count_2: ArrayD<f64> = count(&data, &None::<ArrayD<f64>>);
//     // println!("{:?}", count_2);
//     //
//     // //////////////////
//     // // boolean data //
//     // //////////////////
//     // let data_bool = arr1(&[true, true, false, false, true]).into_dyn();
//     // let bool_vals = arr1(&[true, false]).into_dyn();
//     // let bool_count: ArrayD<f64> = count(&data_bool, &Some(bool_vals));
//     // println!("{:?}", bool_count);
//     // ```

//     if Option::is_some(&group_by) {
//         let mut count_vec: Vec<f64> = Vec::with_capacity(group_by.as_ref().unwrap().len());
//         for i in 0..group_by.as_ref().unwrap().len() {
//             count_vec.push(data.iter().filter(|&elem| *elem == group_by.as_ref().unwrap()[i]).count() as f64);        }
//         return arr1(&count_vec).into_dyn();
//     } else {
//         return arr1(&[data.len() as f64]).into_dyn();
//     }
// }

/// Accepts bin edges and bin definition rule and returns an array of bin names
///
/// # Arguments
/// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
/// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
///                      If false, then bins are closed on the right.
///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
///                      regardless of the value of `inclusive_left`.
///
/// Return
/// Array of bin names.
///
/// Example
/// ```
/// use yarrow_runtime::utilities::aggregations::get_bin_names;
/// use ndarray::prelude::*;
/// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
/// let inclusive_left: bool = true;
/// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
/// println!("{}", bin_names);
/// ```
pub fn get_bin_names(edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {

    let mut bin_name_vec: Vec<String> = Vec::with_capacity(edges.len()-1);
    let mut left_edge = String::new();
    let mut right_edge = String::new();
    let mut bin_name = String::new();
    for i in 0..(edges.len()-1) {
        left_edge = edges[i].to_string();
        right_edge = edges[i+1].to_string();
        if (i == 0 && inclusive_left == &false) {
            bin_name = format!("[{}, {}]", left_edge, right_edge);
        } else if (i == (edges.len()-2) && inclusive_left == &true) {
            bin_name = format!("[{}, {}]", left_edge, right_edge);
        } else if inclusive_left == &true {
            bin_name = format!("[{}, {})", left_edge, right_edge);
        } else {
            bin_name = format!("({}, {}]", left_edge, right_edge);
        }
        bin_name_vec.push(bin_name);
    }
    return arr1(&bin_name_vec).into_dyn();
}

// pub fn histogram(data: &ArrayD<f64>, edges: &ArrayD<f64>, inclusive_left: &bool) -> HashMap::<String, f64> {
//     /// Accepts data, bin edges, and a bin definition rule and returns a HashMap of
//     /// bin names and counts
//     ///
//     /// # Arguments
//     /// * `data` - Array of numeric data to be binned
//     /// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
//     /// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
//     ///                      If false, then bins are closed on the right.
//     ///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
//     ///                      regardless of the value of `inclusive_left`.
//     ///
//     /// # Return
//     /// Hashmap of bin names and counts
//     ///
//     /// # Example
//     /// ```
//     /// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
//     /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
//     /// let inclusive_left: bool = true;
//     /// let hist = histogram(&data, &edges, &inclusive_left);
//     /// println!("{:?}", hist);
//     /// ```

//     // map data to bins
//     let binned_data: ArrayD<String> = transformations::bin(data, edges, inclusive_left);

//     // construct bin names
//     let mut bin_names: ArrayD<String> = get_bin_names(edges, inclusive_left);
//     let mut bin_names_copy: ArrayD<String> = bin_names.clone();

//     // get counts for each bin
//     let mut bin_counts: ArrayD<f64> = count(&binned_data, &Some(bin_names));

//     // construct hashmap of bin_name: count pairs
//     let mut hist_hashmap: HashMap::<String, f64> = HashMap::new();
//     for pair in bin_names_copy.iter().zip(bin_counts.iter_mut()) {
//         let (name, count) = pair;
//         hist_hashmap.insert(name.to_string(), *count);
//     }
//     return hist_hashmap;
// }

/// Accepts data and returns median
///
/// # Arguments
/// * `data` - Array of data for which you would like the median
///
/// # Return
/// median of your data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::median;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let median: ArrayD<f64> = median(&data);
/// println!("{}", median);
/// ```
pub fn median(data: &ArrayD<f64>) -> ArrayD<f64> {

    // create vector version of data, get length, and sort it
    let mut data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let n = data_vec.len();
    data_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // if even number of elements, return mean of the two middlemost elements
    // if odd number of elements, return middlemost element
    if n % 2 == 0 {
        return arr1(&[(data_vec[n/2 - 1] + data_vec[n/2]) / 2.0]).into_dyn();
    } else {
        return arr1(&[data_vec[n/2]]).into_dyn();
    }
}

/// Accepts data and returns sum
///
/// # Arguments
/// * `data` - Array of data for which you would like the sum
///
/// # Return
/// sum of the data
///
/// # Examples
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::sum;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let sum: ArrayD<f64> = sum(&data);
/// println!("{}", sum);
/// ```
pub fn sum(data: &ArrayD<f64>) -> ArrayD<f64> {

    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_sum: f64 = data_vec.iter().map(|x| x).sum();
    return arr1(&[data_sum]).into_dyn();
}

/// Accepts data and returns mean
///
/// # Arguments
/// * `data` - Array of data for which you would like the mean
///
/// # Return
/// mean of the data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::mean;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let mean: ArrayD<f64> = mean(&data);
/// println!("{}", mean);
/// ```
pub fn mean(data: &ArrayD<f64>) -> ArrayD<f64> {

    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_vec_len: f64 = data_vec.len() as f64;
    let data_sum: f64 = data_vec.iter().map(|x| x).sum();
    return arr1(&[data_sum / data_vec_len]).into_dyn();
}

/// Accepts data and returns variance
///
/// # Arguments
/// * `data` - Array of data for which you would like the variance
/// * `finite_sample_correction` - Whether or not to calculate variance with finite sample correction
///
/// # Return
/// variance of the data
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::variance;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let variance: ArrayD<f64> = variance(&data, &false);
/// println!("{}", variance);
/// ```
pub fn variance(data: &ArrayD<f64>, finite_sample_correction: &bool) -> ArrayD<f64> {

    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_vec_len: f64 = data_vec.len() as f64;
    let expectation_data_squared: ArrayD<f64> = arr1(&[mean(&data).into_dimensionality::<Ix1>().unwrap().to_vec()[0].powf(2.)]).into_dyn();
    let squared_data: Vec<f64> = data_vec.iter().map(|x| x.powf(2.)).collect();
    let expectation_squared_data: ArrayD<f64> = mean(&arr1(&squared_data).into_dyn());

    let mut variance: f64 = (expectation_squared_data - expectation_data_squared).into_dimensionality::<Ix1>().unwrap().to_vec()[0];
    if finite_sample_correction == &true {
        variance *= (&data_vec_len / (&data_vec_len - &1.));
    }
    return arr1(&[variance]).into_dyn();
}

/// Accepts data and returns sample estimate of kth raw moment
///
/// # Arguments
/// * `data` - Array of data for which you would like the kth raw moment
/// * `k` - integer representing moment you want
///
/// # Return
/// kth sample moment
///
/// # Example
/// ```
/// use ndarray::prelude::*;
/// use yarrow_runtime::utilities::aggregations::kth_raw_sample_moment;
/// let data: ArrayD<f64> = arr1(&[0., 1., 2., 3., 4., 5., 12., 19., 24., 90., 98., 100.]).into_dyn();
/// let third_moment: ArrayD<f64> = kth_raw_sample_moment(&data, &3);
/// println!("{}", third_moment);
/// ```
pub fn kth_raw_sample_moment(data: &ArrayD<f64>, k: &i64) -> ArrayD<f64> {

    assert!(k >= &0);
    let data_vec: Vec<f64> = data.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let data_to_kth_power: Vec<f64> = data_vec.iter().map(|x| x.powf(*k as f64)).collect();
    return mean(&arr1(&data_to_kth_power).into_dyn());
}

pub fn create_subset<T>(set: &ArrayD<T>, weights: &ArrayD<f64>, k: &u64) -> ArrayD<T> where T: Clone {
    /// Accepts set and element probabilities and returns a subset of size k
    /// Probabilities are the probability of drawing each element on the first draw (they sum to 1)
    /// Based on Algorithm A from Raimidis PS, Spirakis PG (2006). “Weighted random sampling with a reservoir.”

    assert!(*k as usize <= set.len());

    let mut set_vec: Vec<T> = set.clone().into_dimensionality::<Ix1>().unwrap().to_vec();

    let mut weights_vec: Vec<f64> = weights.clone().into_dimensionality::<Ix1>().unwrap().to_vec();
    let weights_sum: f64 = weights_vec.iter().sum();

    let mut probabilities_vec: Vec<f64> = weights.iter().map(|w| w / weights_sum).collect();
    let mut subsample_vec: Vec<T> = Vec::with_capacity(*k as usize);

    //
    // generate keys and identify top k indices
    //

    // generate key/index tuples
    let mut key_vec = Vec::with_capacity(*k as usize);
    for i in 0..*k {
        key_vec.push( (noise::sample_uniform(0., 1.).powf(1./probabilities_vec[i as usize]), i) );
    }

    // sort key/index tuples by key and identify top k indices
    key_vec.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let mut top_indices: Vec<i64> = Vec::with_capacity(*k as usize);
    for i in 0..*k {
        top_indices.push(key_vec[i as usize].1 as i64);
    }

    // subsample based on top k indices
    let mut subset: Vec<T> = Vec::with_capacity(*k as usize);
    for value in top_indices.iter().map(|&index| set_vec[index as usize].clone()) {
        subset.push(value);
    }
    return arr1(&subset).into_dyn();
}

pub fn create_sampling_indices(k: &u64, n: &u64) -> ArrayD<u64> {
    /// Creates set of indices for subsampling from data without replacement

    // create set of all indices
    let mut index_vec: Vec<u64> = Vec::with_capacity(*n as usize);
    for i in 0..*n {
        index_vec.push(i as u64);
    }
    let index_array: ArrayD<u64> = arr1(&index_vec).into_dyn();

    // create uniform selection probabilities
    let prob_array: ArrayD<f64> = arr1(&vec![1./(*n as f64); *n as usize]).into_dyn();

    // create set of sampling indices
    let sampling_indices: ArrayD<u64> = create_subset(&index_array, &prob_array, k);

    return sampling_indices;
}

// pub fn create_sampling_indices(k: &i64, n: &i64) -> ArrayD<u64> {
//     // create vector of all indices
//     let mut index_vec: Vec<i64> = Vec::with_capacity(*n as usize);
//     for i in 0..*n {
//         index_vec.push(i);
//     }

//     //
//     // generate keys and identify k indices
//     //

//     // generate key/index tuples
//     let mut key_vec: Vec<f64> = Vec::with_capacity(*n as usize);
//     for i in 0..*n {
//         key_vec.push( (noise::sample_uniform(0., 1.).powf(*n as f64), i) );
//     }

//     // sort key/index tuples by key and identify k indices
//     key_vec.sort_by(|a, b| b.partial_cmp(a).unwrap());
//     let mut indices: Vec<i64> = Vec::with_capacity(*k as usize);
//     for i in 0..*k {
//         indices.push(key_vec[i as usize].1 as u64);
//     }

//     return arr1(&indices).into_dyn();
// }