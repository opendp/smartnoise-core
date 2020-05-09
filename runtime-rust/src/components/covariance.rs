use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::ReleaseNode;
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};

use whitenoise_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;
use std::iter::FromIterator;

impl Evaluable for proto::Covariance {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let delta_degrees_of_freedom = if self.finite_sample_correction {1} else {0} as usize;
        if arguments.contains_key("data") {
            let data = get_argument(arguments, "data")?.array()?.f64()?;
            let covariances = matrix_covariance(&data, &delta_degrees_of_freedom)?.into_iter()
                .flat_map(|x| x)
                .collect::<Vec<f64>>();

            // flatten into a row vector, every column is a release
            return Ok(ReleaseNode::new(arr1(&covariances).insert_axis(Axis(0)).into_dyn().into()));
        }
        if arguments.contains_key("left") && arguments.contains_key("right") {
            let left = get_argument(arguments, "left")?.array()?.f64()?;
            let right = get_argument(arguments, "right")?.array()?.f64()?;

            let cross_covariances = matrix_cross_covariance(&left, &right, &delta_degrees_of_freedom)?;

            // flatten into a row vector, every column is a release
            return Ok(ReleaseNode::new(Array::from_iter(cross_covariances.iter())
                .insert_axis(Axis(0)).into_dyn().mapv(|v| v.clone()).into()));
        }
        Err("insufficient data supplied to Covariance".into())
    }
}

/// Construct upper triangular of covariance matrix from data matrix.
///
/// # Arguments
/// * `data` - Data for which you want covariance matrix.
/// * `delta_degrees_of_freedom` - 0 for population, 1 for finite sample correction
///
/// # Return
/// Upper triangular of covariance matrix of your data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2};
/// use whitenoise_runtime::components::covariance::matrix_covariance;
///
/// // covariance matrix is:
/// // [12.5  7.5 -7.5]
/// // [ 7.5  4.5 -4.5]
/// // [-7.5 -4.5  4.5]
///
/// let data = arr2(&[ [0., 2., 9.], [5., 5., 6.] ]).into_dyn();
/// let cov_mat = matrix_covariance(&data, &1).unwrap();
/// assert!(cov_mat == vec![ vec![12.5, 7.5, -7.5], vec![4.5, -4.5], vec![4.5] ]);
/// ```
pub fn matrix_covariance(data: &ArrayD<f64>, delta_degrees_of_freedom: &usize) -> Result<Vec<Vec<f64>>> {

    let means: Vec<f64> = mean(&data)?.iter().cloned().collect();

    let mut covariances: Vec<Vec<f64>> = Vec::new();
    data.gencolumns().into_iter().enumerate()
        .for_each(|(left_i, left_col)| {
            let mut col_covariances: Vec<f64> = Vec::new();
            data.gencolumns().into_iter().enumerate()
                .filter(|(right_i, _right_col)| &left_i <= right_i)
                .for_each(|(right_i, right_col)|
                    col_covariances.push(covariance(
                        &left_col, &right_col,
                        &means[left_i].clone(), &means[right_i].clone(),
                        delta_degrees_of_freedom)));
            covariances.push(col_covariances);
        });

    Ok(covariances)
}

/// Construct cross-covariance matrix from pair of data matrices.
///
/// Element (i,j) of the cross-covariance matrix will be the covariance of the
/// column i of `left` and column `j` of `right`
///
/// # Arguments
/// * `left` - One of the two matrices for which you want the cross-covariance matrix.
/// * `right` - One of the two matrices for which you want the cross-covariance matrix.
/// * `delta_degrees_of_freedom` - 0 for population, 1 for finite sample correction
///
/// # Return
/// Full cross-covariance matrix.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2};
/// use whitenoise_runtime::components::covariance::matrix_cross_covariance;
///
/// let left = arr2(&[ [1., 3., 5.,], [2., 4., 6.] ]).into_dyn();
/// let right = arr2(&[ [2., 4., 6.], [1., 3., 5.] ]).into_dyn();
///
/// let cross_covar = matrix_cross_covariance(&left, &right, &(1 as usize)).unwrap();
/// let left_covar = matrix_cross_covariance(&left, &left, &(1 as usize)).unwrap();
///
/// // cross-covariance of left and right matrices
/// assert!(cross_covar == arr2(&[ [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5] ]).into_dyn());
///
/// // cross-covariance of left with itself is equivalent to the standard covariance matrix
/// assert!(left_covar == arr2(&[ [0.5, 0.5, 0.5], [0.5, 0.5, 0.5], [0.5, 0.5, 0.5] ]).into_dyn());
/// ```
pub fn matrix_cross_covariance(
    left: &ArrayD<f64>, right: &ArrayD<f64>,
    delta_degrees_of_freedom: &usize
) -> Result<ArrayD<f64>> {

    let left_means: Vec<f64> = mean(&left)?.iter().cloned().collect();
    let right_means: Vec<f64> = mean(&right)?.iter().cloned().collect();

    let covariances = left.gencolumns().into_iter()
        .zip(left_means.iter())
        .flat_map(|(column_left, mean_left)|
            right.gencolumns().into_iter()
                .zip(right_means.iter())
                .map(|(column_right, mean_right)| covariance(
                    &column_left, &column_right,
                    &mean_left, &mean_right,
                &delta_degrees_of_freedom))
                .collect::<Vec<f64>>())
        .collect::<Vec<f64>>();

    match Array::from_shape_vec((left_means.len(), right_means.len()), covariances) {
        Ok(array) => Ok(array.into_dyn()),
        Err(_) => Err("unable to form cross-covariance matrix".into())
    }
}

/// Get covariance between two 1D-arrays.
///
/// # Arguments
/// * `left` - One of the two arrays for which you want the covariance.
/// * `right` - One of the two arrays for which you want the covariance.
/// * `mean_left` - Arithmetic mean of the left array.
/// * `mean_right` - Arithmetic mean of the right array.
/// * `delta_degrees_of_freedom` - 0 for population, 1 for finite sample correction
///
/// # Return
/// Covariance of the two arrays.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1};
/// use whitenoise_runtime::components::covariance::covariance;
///
/// let left = arr1(&[1.,2.,3.]);
/// let right = arr1(&[4.,5.,6.]);
/// let mean_left = 2.;
/// let mean_right = 5.;
/// let cov = covariance(&left.view(), &right.view(), &mean_left, &mean_right, &1);
/// assert!(cov == 1.);
/// ```
pub fn covariance(left: &ArrayView1<f64>, right: &ArrayView1<f64>, mean_left: &f64, mean_right: &f64, delta_degrees_of_freedom: &usize) -> f64 {
    left.iter()
        .zip(right)
        .fold(0., |sum, (val_left, val_right)|
            sum + ((val_left - mean_left) * (val_right - mean_right))) / ( (left.len() - delta_degrees_of_freedom.clone()) as f64)
}