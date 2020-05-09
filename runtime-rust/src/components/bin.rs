use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, Array, Jagged};
use crate::components::Evaluable;
use ndarray::{ArrayD};
use whitenoise_validator::proto;
use crate::utilities::get_num_columns;
use std::ops::{Div, Add};
use whitenoise_validator::utilities::{get_argument, standardize_categorical_argument, standardize_numeric_argument, standardize_float_argument};

impl Evaluable for proto::Bin {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<Value> {
        let inclusive_left: &ArrayD<bool> = get_argument(arguments, "inclusive_left")?.array()?.bool()?;

        let side = match self.side.as_str() {
            "lower" => BinSide::Lower,
            "midpoint" => BinSide::Midpoint,
            "upper" => BinSide::Upper,
            _ => return Err("bin side must be lower, midpoint or upper".into())
        };

        let data = get_argument(arguments, "data")?.array()?;
        let edges = get_argument(arguments, "edges")?.jagged()?;
        let null = get_argument(arguments, "null_value")?.array()?;

        let num_columns = data.num_columns()?;

        match (data, edges, null) {
            (Array::F64(data), Jagged::F64(edges), Array::F64(null)) =>
                Ok(bin(&data, standardize_float_argument(edges, &num_columns)?, &inclusive_left, &null, &side)?.into()),

            (Array::I64(data), Jagged::I64(edges), Array::I64(null)) =>
                Ok(bin(&data, standardize_categorical_argument(edges, &num_columns)?, &inclusive_left, &null, &side)?.into()),

            _ => return Err("data and edges must both be f64 or i64".into())
        }
    }
}

pub enum BinSide {
    Lower,
    Midpoint,
    Upper,
}

/// Maps data to bins.
///
/// Bins will be of the form [lower, upper) or (lower, upper].
///
/// # Arguments
/// * `data` - Data to be binned.
/// * `edges` - Values representing the edges of bins.
/// * `inclusive_left` - Whether or not the left edge of the bin is inclusive, i.e. the bins are of the form [lower, upper).
/// * `null` - Value to which to map if there is no valid bin (e.g. if the element falls outside the bin range).
/// * `side` - How to refer to each bin. Will be either the `left` edge, the `right` edge, or the `center` (the arithmetic mean of the two).
///
/// # Return
/// Binned data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::bin::{bin, BinSide};
///
/// let data = arr1(&[1.1, 2., 2.9, 4.1, 6.4]).into_dyn();
/// let edges = vec![vec![0., 1., 2., 3., 4., 5.]];
/// let inclusive_left = arr1(&[true]).into_dyn();
/// let null = arr1(&[-1.]).into_dyn();
/// let side = BinSide::Midpoint;
///
/// let binned = bin(&data, edges, &inclusive_left, &null, &side).unwrap();
/// assert!(binned == arr1(&[1.5, 2.5, 2.5, 4.5, -1.]).into_dyn());
/// ```
pub fn bin<T: std::cmp::PartialOrd + Clone + Div<T, Output=T> + Add<T, Output=T> + From<i32> + Copy>(
    data: &ArrayD<T>,
    edges: Vec<Vec<T>>,
    inclusive_left: &ArrayD<bool>,
    null: &ArrayD<T>,
    side: &BinSide
)-> Result<ArrayD<T>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    let inclusive_left = standardize_numeric_argument(&inclusive_left, &num_columns)?;
    let null = standardize_numeric_argument(&null, &num_columns)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(edges.into_iter().zip(null.into_iter()))
        .zip(inclusive_left.iter())
        // for each pairing, iterate over the cells
        .for_each(|((mut column, (mut edges, null)), inclusive_left)| {
            edges.sort_by(|a, b| a.partial_cmp(b).unwrap());
            column.iter_mut()
                // mutate the cell via the operator
                .for_each(|v| {
                    // checks for nullity
                    if edges.len() == 0 || *v < edges[0] || *v > edges[edges.len() - 1] {
                        *v = null.clone();
                        return;
                    }

                    // assign to edge
                    for idx in 0..(edges.len() - 1) {
                        // check whether left or right side of bin should be considered inclusive
                        if match inclusive_left {
                            true => edges[idx] <= *v && *v < edges[idx + 1],
                            false => edges[idx] < *v && *v <= edges[idx + 1]
                        } {
                            // assign element a new name based on bin naming rule
                            *v = match side {
                                BinSide::Lower => edges[idx],
                                BinSide::Upper => edges[idx + 1],
                                BinSide::Midpoint => (edges[idx] / T::from(2)) + (edges[idx + 1] / T::from(2))
                            };
                            return;
                        }
                    }
                    *v = edges[edges.len() - 1];
                })
        });

    Ok(data)
}

//pub fn bin<T>(data: &ArrayD<T>, edges: &ArrayD<T>, inclusive_left: &ArrayD<bool>)
//              -> Result<ArrayD<String>> where T: Clone, T: PartialOrd, T: std::fmt::Display {
//    /// Accepts vector of data and assigns each element to a bin
//    /// NOTE: bin transformation has C-stability of 1
//    ///
//    /// # Arguments
//    /// * `data` - Array of numeric data to be binned
//    /// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
//    /// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
//    ///                      If false, then bins are closed on the right.
//    ///                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
//    ///                      regardless of the value of `inclusive_left`.
//    ///
//    /// # Return
//    /// ArrayD of bin assignments
//    ///
//    /// # Example
//    /// ```
//    /// // set up data
//    /// use ndarray::{ArrayD, arr1, Array1};
//    /// use whitenoise_runtime::utilities::transformations::bin;
//    /// let data: ArrayD<f64> = arr1(&[1., 2., 3., 4., 5., 12., 19., 24., 90., 98.]).into_dyn();
//    /// let edges: ArrayD<f64> = arr1(&[0., 10., 20., 30., 40., 50., 60., 70., 80., 90., 100.]).into_dyn();
//    /// let inclusive_left: ArrayD<bool> = arr1(&[false]).into_dyn();
//    ///
//    /// // bin data
//    /// let binned_data: ArrayD<String> = bin(&data, &edges, &inclusive_left)?;
//    /// println!("{:?}", binned_data);
//    /// ```
//
//
//    // initialize new data -- this is what we ultimately return from the function
//    let original_dim: u8 = data.ndim() as u8;
//    let new_data: ArrayD<T> = convert_to_matrix(data);
//    let mut new_bin_array: ArrayD<String> = Array::default(new_data.shape());
//
//    let n_cols: i64 = data.len_of(Axis(0)) as i64;
//
//    for k in 0..n_cols {
//        // create vector versions of data and edges
//        let data_vec: Vec<T> = data.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>()?.to_vec();
//        let mut sorted_edges: Vec<T> = edges.slice(s![k as usize, ..]).clone().into_dimensionality::<Ix1>()?.to_vec();
//
//        //  ensure edges are sorted in ascending order
//        sorted_edges.sort_by(|a, b| a.partial_cmp(b).unwrap());
//
//        // initialize output vector
//        let mut bin_vec: Vec<String> = Vec::with_capacity(data_vec.len());
//
//        // for each data element, look for correct bin and append name to bin_vec
//        for i in 0..data_vec.len() {
//            // append empty string if data are outside of bin ranges
//            if data_vec[i] < sorted_edges[0] || data_vec[i] > sorted_edges[sorted_edges.len()-1] {
//                bin_vec.push("".to_string());
//            } else {
//                // for each bin
//                for j in 0..(sorted_edges.len()-1) {
//                    if  // element is less than the right bin edge
//                    data_vec[i] < sorted_edges[j+1] ||
//                        // element is equal to the right bin edge and we are building our histogram to be 'right-edge inclusive'
//                        (data_vec[i] == sorted_edges[j+1] && inclusive_left[k as usize] == false) ||
//                        // element is equal to the right bin edge and we are checking our rightmost bin
//                        (data_vec[i] == sorted_edges[j+1] && j == (sorted_edges.len()-2)) {
//                        if j == 0 && inclusive_left[k as usize] == false {
//                            // leftmost bin must be left inclusive even if overall strategy is to be right inclusive
//                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
//                        } else if j == (sorted_edges.len()-2) && inclusive_left[k as usize] == true {
//                            // rightmost bin must be right inclusive even if overall strategy is to be left inclusive
//                            bin_vec.push(format!("[{}, {}]", sorted_edges[j], sorted_edges[j+1]));
//                        } else if inclusive_left[k as usize] == true {
//                            bin_vec.push(format!("[{}, {})", sorted_edges[j], sorted_edges[j+1]));
//                        } else {
//                            bin_vec.push(format!("({}, {}]", sorted_edges[j], sorted_edges[j+1]));
//                        }
//                        break;
//                    }
//                }
//            }
//        }
//        // convert bin vector to Array and return
//        let bin_array: ArrayD<String> = arr1(&bin_vec).into_dyn();
//        new_bin_array.slice_mut(s![k as usize, ..]).assign(&bin_array);
//    }
//    return Ok(convert_from_matrix(&new_bin_array, &original_dim));
//}
//
//
//
//
///// Accepts bin edges and bin definition rule and returns an array of bin names
/////
///// # Arguments
///// * `edges` - Array of bin edges, an array of n+1 edges will yield n bins
///// * `inclusive_left` - Boolean for whether or not bins (representing numeric intervals) are closed on the left.
/////                      If false, then bins are closed on the right.
/////                      The leftmost and rightmost bins are automatically closed on the left/right (respectively),
/////                      regardless of the value of `inclusive_left`.
/////
///// # Return
///// Array of bin names.
/////
///// Example
///// ```
///// use whitenoise_runtime::utilities::aggregations::get_bin_names;
///// use ndarray::prelude::*;
///// let edges: ArrayD<f64> = arr1(&[0., 10., 20.]).into_dyn();
/////
///// let inclusive_left: bool = true;
///// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
///// assert_eq!(arr1(&["[0, 10)", "[10, 20]"]).into_dyn(), bin_names);
/////
///// let inclusive_left: bool = false;
///// let bin_names: ArrayD<String> = get_bin_names(&edges, &inclusive_left);
///// assert_eq!(arr1(&["[0, 10]", "(10, 20]"]).into_dyn(), bin_names);
///// ```
//pub fn get_bin_names(edges: &ArrayD<f64>, inclusive_left: &bool) -> ArrayD<String> {
//
//    let mut bin_name_vec: Vec<String> = Vec::with_capacity(edges.len()-1);
//    let mut left_edge = String::new();
//    let mut right_edge = String::new();
//    let mut bin_name = String::new();
//    for i in 0..(edges.len()-1) {
//        left_edge = edges[i].to_string();
//        right_edge = edges[i+1].to_string();
//        if i == 0 && inclusive_left == &false {
//            bin_name = format!("[{}, {}]", left_edge, right_edge);
//        } else if i == (edges.len()-2) && inclusive_left == &true {
//            bin_name = format!("[{}, {}]", left_edge, right_edge);
//        } else if inclusive_left == &true {
//            bin_name = format!("[{}, {})", left_edge, right_edge);
//        } else {
//            bin_name = format!("({}, {}]", left_edge, right_edge);
//        }
//        bin_name_vec.push(bin_name);
//    }
//    return arr1(&bin_name_vec).into_dyn();
//}
