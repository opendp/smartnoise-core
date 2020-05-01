use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Array, Jagged, ReleaseNode};
use crate::components::Evaluable;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use crate::utilities::get_num_columns;
use std::ops::{Div, Add};
use whitenoise_validator::utilities::{get_argument, standardize_categorical_argument, standardize_numeric_argument, standardize_float_argument};

impl Evaluable for proto::Digitize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let inclusive_left: &ArrayD<bool> = get_argument(&arguments, "inclusive_left")?.array()?.bool()?;

        let data = get_argument(&arguments, "data")?.array()?;
        let edges = get_argument(&arguments, "edges")?.jagged()?;
        let null = get_argument(&arguments, "null_value")?.array()?.i64()?;
        let num_columns = data.num_columns()?;

        Ok(ReleaseNode::new(match (data, edges) {
            (Array::F64(data), Jagged::F64(edges)) =>
                digitize(&data, &standardize_float_argument(edges, &num_columns)?, &inclusive_left, &null)?.into(),

            (Array::I64(data), Jagged::I64(edges)) =>
                digitize(&data, &standardize_categorical_argument(edges.clone(), &num_columns)?, &inclusive_left, &null)?.into(),

            _ => return Err("data and edges must both be f64 or i64".into())
        }))
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
/// use whitenoise_runtime::components::digitize::{bin_index, digitize};
/// use whitenoise_validator::utilities::standardize_float_argument;
/// use whitenoise_runtime::utilities::get_num_columns;
///
/// let data = arr1(&[1.1, 2., 2.9, 4.1, 6.4]).into_dyn();
/// let edges = vec![Some(vec![0., 1., 2., 3., 4., 5.])];
/// let inclusive_left = arr1(&[true]).into_dyn();
/// let null = arr1(&[-1]).into_dyn();
///
///
/// let num_columns = get_num_columns(&data).unwrap();
/// let edges = standardize_float_argument(&edges, &num_columns).unwrap();
///
/// let digitization = digitize(&data, &edges, &inclusive_left, &null).unwrap();
/// println!("digitize {:?}", digitization);
/// assert!(digitization == arr1(&[1, 2, 2, 4, -1]).into_dyn());
/// ```
pub fn digitize<T: std::cmp::PartialOrd + Clone + Div<T, Output=T> + Add<T, Output=T> + From<i32> + Copy + Default>(
    data: &ArrayD<T>,
    edges: &Vec<Vec<T>>,
    inclusive_left: &ArrayD<bool>,
    null: &ArrayD<i64>,
) -> Result<ArrayD<i64>> {
    let mut digitization = ArrayD::default(data.shape());

    let num_columns = get_num_columns(&data)?;

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

/// Given datum and bin definition, finds index of appropriate bin.
///
/// Bins will be of the form [lower, upper) or (lower, upper] and are constructed
/// from `edges` and `inclusive_left`.
///
/// # Arguments
/// * `data` - Data to be binned.
/// * `edges` - Values representing the edges of bins.
/// * `inclusive_left` - Whether or not the left edge of the bin is inclusive, i.e. the bins are of the form [lower, upper).
///
/// # Return
/// Index of appropriate bin.
///
/// # Example
/// ```
/// use whitenoise_runtime::components::digitize::bin_index;
///
/// let data = vec![1.1, 2., 2.9, 4.1, 6.4];
/// let edges = vec![0., 1., 2., 3., 4., 5.];
///
/// let index1 = bin_index(&data[1], &edges, &true);
/// assert!(index1 == Some(2));
/// let index2 = bin_index(&data[1], &edges, &false);
/// assert!(index2 == Some(1));
/// let index3 = bin_index(&data[4], &edges, &true);
/// assert!(index3.is_none());
/// ```
pub fn bin_index<T: PartialOrd + Clone>(
    datum: &T,
    edges: &Vec<T>,
    inclusive_left: &bool,
) -> Option<usize> {
    // checks for nullity
    if edges.len() == 0 || datum < &edges[0] || datum > &edges[edges.len() - 1] {
        return None;
    }

    match inclusive_left {
        true => if datum == &edges[edges.len() - 1] {return None},
        false => if datum == &edges[0] {return None}
    }
    // assign to edge
    let mut l: usize = 0;
    let mut r: usize = edges.len() - 2;
    let mut idx: usize = 0;
    while l <= r {
        idx = (l + r) / 2;
        match inclusive_left {
            true => {
                if &edges[idx + 1] <= datum {
                    l = idx + 1;
                } else if &edges[idx] > datum {
                    r = idx - 1;
                } else {
                    break
                }
            },
            false =>
                if &edges[idx + 1] < datum {
                    l = idx + 1;
                } else if &edges[idx] >= datum {
                    r = idx - 1;
                } else {
                    break
                }
        }
    }
    return Some(idx);
}

#[cfg(test)]
mod test_bin_index {
    use crate::components::digitize::bin_index;

    #[test]
    fn test_edges() {

        let data = vec![-1., 0., 1.1, 2., 2.9, 4.1, 5., 6.4];
        let edges = vec![0., 1., 2., 3., 4., 5.];

        data.iter()
            .zip(vec![None, Some(0), Some(1), Some(2), Some(2), Some(4), None, None].iter())
            .for_each(|(datum, truth)| {
//                println!("{}, {:?}", datum, truth);
                assert!(bin_index(datum, &edges, &true) == *truth);
            });

        data.iter()
            .zip(vec![None, None, Some(1), Some(1), Some(2), Some(4), Some(4), None].iter())
            .for_each(|(datum, truth)|
                assert!(bin_index(datum, &edges, &false) == *truth));
    }
}