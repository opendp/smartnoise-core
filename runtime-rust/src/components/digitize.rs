use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, Array, Jagged};
use crate::components::Evaluable;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use crate::utilities::get_num_columns;
use std::ops::{Div, Add};
use whitenoise_validator::utilities::{get_argument, standardize_categorical_argument, standardize_numeric_argument};
use std::fmt::Display;

impl Evaluable for proto::Digitize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let inclusive_left: &ArrayD<bool> = get_argument(&arguments, "inclusive_left")?.array()?.bool()?;

        let data = get_argument(&arguments, "data")?.array()?;
        let edges = get_argument(&arguments, "edges")?.jagged()?;
        let null = get_argument(&arguments, "null")?.array()?.i64()?;

        match (data, edges) {
            (Array::F64(data), Jagged::F64(edges)) =>
                Ok(digitize(&data, &edges, &inclusive_left, &null)?.into()),
            (Array::I64(data), Jagged::I64(edges)) =>
                Ok(digitize(&data, &edges, &inclusive_left, &null)?.into()),
            _ => return Err("data and edges must both be f64 or i64".into())
        }
    }
}

/// Maps data in bins to digits.
///
/// Bins will be of the form [lower, upper) or (lower, upper].
///
/// # Arguments
/// * `data` - Data to be binned.
/// * `edges` - Values representing the edges of bins.
/// * `inclusive_left` - Whether or not the left edge of the bin is inclusive, i.e. the bins are of the form [lower, upper).
/// * `null` - Value to which to map if there is no valid bin (e.g. if the element falls outside the bin range).
///
/// # Return
/// Binned data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use whitenoise_runtime::components::digitize::{digitize};
///
/// let data = arr1(&[1.1, 2., 2.9, 4.1, 6.4]).into_dyn();
/// let edges = vec![Some(vec![0., 1., 2., 3., 4., 5.])];
/// let inclusive_left = arr1(&[true]).into_dyn();
/// let null = arr1(&[-1]).into_dyn();
///
/// let digitization = digitize(&data, &edges, &inclusive_left, &null).unwrap();
/// println!("digitize {:?}", digitization);
/// assert!(digitization == arr1(&[1, 2, 2, 4, -1]).into_dyn());
/// ```
pub fn digitize<T: std::fmt::Debug + Display + std::cmp::PartialOrd + Clone + Div<T, Output=T> + Add<T, Output=T> + From<i32> + Copy + Default>(
    data: &ArrayD<T>,
    edges: &Vec<Option<Vec<T>>>,
    inclusive_left: &ArrayD<bool>,
    null: &ArrayD<i64>,
) -> Result<ArrayD<i64>> {
    let mut digitization = ArrayD::default(data.shape());

    let num_columns = get_num_columns(&data)?;

    let edges = standardize_categorical_argument(&edges, &num_columns)?;
    let inclusive_left = standardize_numeric_argument(&inclusive_left, &num_columns)?;
    let null = standardize_numeric_argument(&null, &num_columns)?;

    // iterate over the generalized columns
    digitization.gencolumns_mut().into_iter()
        .zip(data.gencolumns().into_iter())
        // pair generalized columns with arguments
        .zip(edges.into_iter().zip(null.into_iter()))
        .zip(inclusive_left.iter())
        // for each pairing, iterate over the cells
        .for_each(|(((mut col_dig, col_data), (edges, null)), inclusive_left)|
            col_dig.iter_mut().zip(col_data.iter()).for_each(|(digit, datum)|
                // mutate the cell via the operator
                *digit = bin_index(datum, &edges, inclusive_left)
                    .map(|v| v as i64)
                    .unwrap_or_else(|| null.clone())));

    Ok(digitization)
}

// TODO: switch to binary search, for efficiency when bin set is large
pub fn bin_index<T: std::fmt::Debug + Display + std::cmp::PartialOrd + Clone + Div<T, Output=T> + Add<T, Output=T> + From<i32> + Copy>(
    datum: &T,
    edges: &Vec<T>,
    inclusive_left: &bool,
) -> Option<usize> {
    // checks for nullity
    if edges.len() == 0 || *datum < edges[0] || *datum > edges[edges.len() - 1] {
        return None;
    }

    // assign to edge
    for idx in 0..(edges.len() - 1) {
        // check whether left or right side of bin should be considered inclusive
        if match inclusive_left {
            true => edges[idx] <= *datum && *datum < edges[idx + 1],
            false => edges[idx] < *datum && *datum <= edges[idx + 1]
        } {
            return Some(idx);
        }
    }

    Some(edges.len() - 1)
}
