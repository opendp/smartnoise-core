use smartnoise_validator::errors::*;

use crate::NodeArguments;
use smartnoise_validator::base::{ReleaseNode, IndexKey};
use smartnoise_validator::utilities::take_argument;
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};

use smartnoise_validator::{proto, Float};
use crate::components::mean::mean;
use ndarray::prelude::*;
use std::iter::FromIterator;

impl Evaluable for proto::Covariance {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let delta_degrees_of_freedom = if self.finite_sample_correction {1} else {0} as usize;
        if arguments.contains_key::<IndexKey>(&"data".into()) {
            let data = take_argument(&mut arguments, "data")?.array()?.float()?;
            let covariances = matrix_covariance(&data, delta_degrees_of_freedom)?.into_iter()
                .flatten()
                .collect::<Vec<Float>>();

            // flatten into a row vector, every column is a release
            return Ok(ReleaseNode::new(arr1(&covariances).insert_axis(Axis(0)).into_dyn().into()));
        }
        if arguments.contains_key::<IndexKey>(&"left".into()) && arguments.contains_key::<IndexKey>(&"right".into()) {
            let left = take_argument(&mut arguments, "left")?.array()?.float()?;
            let right = take_argument(&mut arguments, "right")?.array()?.float()?;

            let cross_covariances = matrix_cross_covariance(&left, &right, delta_degrees_of_freedom)?;

            // flatten into a row vector, every column is a release
            return Ok(ReleaseNode::new(Array::from_iter(cross_covariances.iter())
                .insert_axis(Axis(0)).into_dyn().mapv(|v| *v).into()));
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
/// use smartnoise_runtime::components::covariance::matrix_covariance;
///
/// // covariance matrix is:
/// // [12.5  7.5 -7.5]
/// // [ 7.5  4.5 -4.5]
/// // [-7.5 -4.5  4.5]
///
/// let data = arr2(&[ [0., 2., 9.], [5., 5., 6.] ]).into_dyn();
/// let cov_mat = matrix_covariance(&data, 1).unwrap();
/// assert_eq!(cov_mat, vec![ vec![12.5, 7.5, -7.5], vec![4.5, -4.5], vec![4.5] ]);
/// ```
pub fn matrix_covariance(data: &ArrayD<Float>, delta_degrees_of_freedom: usize) -> Result<Vec<Vec<Float>>> {

    let means: Vec<Float> = mean(&data)?.iter().cloned().collect();

    let mut covariances: Vec<Vec<Float>> = Vec::new();
    data.gencolumns().into_iter().enumerate()
        .for_each(|(left_i, left_col)| {
            let mut col_covariances: Vec<Float> = Vec::new();
            data.gencolumns().into_iter().enumerate()
                .filter(|(right_i, _right_col)| &left_i <= right_i)
                .for_each(|(right_i, right_col)|
                    col_covariances.push(covariance(
                        &left_col, &right_col,
                        means[left_i], means[right_i],
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
/// use smartnoise_runtime::components::covariance::matrix_cross_covariance;
///
/// let left = arr2(&[ [1., 3., 5.,], [2., 4., 6.] ]).into_dyn();
/// let right = arr2(&[ [2., 4., 6.], [1., 3., 5.] ]).into_dyn();
///
/// let cross_covar = matrix_cross_covariance(&left, &right, 1).unwrap();
/// let left_covar = matrix_cross_covariance(&left, &left, 1).unwrap();
///
/// // cross-covariance of left and right matrices
/// assert_eq!(cross_covar, arr2(&[ [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5] ]).into_dyn());
///
/// // cross-covariance of left with itself is equivalent to the standard covariance matrix
/// assert_eq!(left_covar, arr2(&[ [0.5, 0.5, 0.5], [0.5, 0.5, 0.5], [0.5, 0.5, 0.5] ]).into_dyn());
/// ```
pub fn matrix_cross_covariance(
    left: &ArrayD<Float>, right: &ArrayD<Float>,
    delta_degrees_of_freedom: usize
) -> Result<ArrayD<Float>> {

    let left_means: Vec<Float> = mean(&left)?.iter().cloned().collect();
    let right_means: Vec<Float> = mean(&right)?.iter().cloned().collect();

    let covariances = left.gencolumns().into_iter()
        .zip(left_means.iter())
        .flat_map(|(column_left, mean_left)|
            right.gencolumns().into_iter()
                .zip(right_means.iter())
                .map(|(column_right, mean_right)| covariance(
                    &column_left, &column_right,
                    *mean_left, *mean_right,
                delta_degrees_of_freedom))
                .collect::<Vec<Float>>())
        .collect::<Vec<Float>>();

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
/// use smartnoise_runtime::components::covariance::covariance;
///
/// let left = arr1(&[1.,2.,3.]);
/// let right = arr1(&[4.,5.,6.]);
/// let mean_left = 2.;
/// let mean_right = 5.;
/// let cov = covariance(&left.view(), &right.view(), mean_left, mean_right, 1);
/// assert_eq!(cov, 1.);
/// ```
pub fn covariance(
    left: &ArrayView1<Float>, right: &ArrayView1<Float>,
    mean_left: Float, mean_right: Float,
    delta_degrees_of_freedom: usize
) -> Float {
    left.iter()
        .zip(right)
        .fold(0., |sum, (val_left, val_right)|
            sum + ((val_left - mean_left) * (val_right - mean_right))) / ( (left.len() - delta_degrees_of_freedom) as Float)
}