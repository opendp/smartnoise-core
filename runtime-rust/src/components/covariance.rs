use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument, Vector2DJagged};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array};

use whitenoise_validator::proto;
use crate::components::mean::mean;
use ndarray::prelude::*;

impl Evaluable for proto::Covariance {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        if arguments.contains_key("data") {
            let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

            return Ok(Value::Vector2DJagged(Vector2DJagged::F64(matrix_covariance(&data)?
                .iter().map(|v| Some(v.clone())).collect())));
        }
        if arguments.contains_key("left") && arguments.contains_key("right") {
            let left = get_argument(&arguments, "left")?.get_arraynd()?.get_f64()?;
            let right = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;

            let cross_cov = matrix_cross_covariance(&left, &right)?;

            return Ok(cross_cov.into());
        }
        Err("insufficient data supplied to Covariance".into())
    }
}

/// Construct upper triangular of sample covariance matrix from data matrix.
///
/// # Arguments
/// * `data` - Data for which you want covariance matrix.
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
/// let cov_mat = matrix_covariance(&data).unwrap();
/// assert!(cov_mat == vec![ vec![12.5, 7.5, -7.5], vec![4.5, -4.5], vec![4.5] ]);
/// ```
pub fn matrix_covariance(data: &ArrayD<f64>) -> Result<Vec<Vec<f64>>> {

    let means: Vec<f64> = mean(&data)?.iter().map(|v| v.clone()).collect();

    let mut covariances: Vec<Vec<f64>> = Vec::new();
    data.gencolumns().into_iter().enumerate()
        .for_each(|(left_i, left_col)| {
            let mut col_covariances: Vec<f64> = Vec::new();
            data.gencolumns().into_iter().enumerate()
                .filter(|(right_i, right_col)| &left_i <= right_i)
                .for_each(|(right_i, right_col)|
                    col_covariances.push(covariance(&left_col, &right_col, &means[left_i].clone(), &means[right_i].clone())));
            covariances.push(col_covariances);
        });
    Ok(covariances)
}

/// Construct sample cross-covariance matrix from pair of data matrices.
///
/// Element (i,j) of the cross-covariance matrix will be the covariance of the
/// column i of `left` and column `j` of `right`
///
/// # Arguments
/// * `left` - One of the two matrices for which you want the cross-covariance matrix.
/// * `right` - One of the two matrices for which you want the cross-covariance matrix.
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
/// let cross_covar = matrix_cross_covariance(&left, &right).unwrap();
/// let left_covar = matrix_cross_covariance(&left, &left).unwrap();
///
/// // cross-covariance of left and right matrices
/// assert!(cross_covar == arr2(&[ [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5], [-0.5, -0.5, -0.5] ]).into_dyn());
///
/// // cross-covariance of left with itself is equivalent to the standard covariance matrix
/// assert!(left_covar == arr2(&[ [0.5, 0.5, 0.5], [0.5, 0.5, 0.5], [0.5, 0.5, 0.5] ]).into_dyn());
pub fn matrix_cross_covariance(left: &ArrayD<f64>, right: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    let left_means: Vec<f64> = mean(&left)?.iter().map(|v| v.clone()).collect();
    let right_means: Vec<f64> = mean(&right)?.iter().map(|v| v.clone()).collect();

    let covariances = left.gencolumns().into_iter()
        .zip(left_means.iter())
        .flat_map(|(column_left, mean_left)|
            right.gencolumns().into_iter()
                .zip(right_means.iter())
                .map(|(column_right, mean_right)| covariance(&column_left, &column_right, &mean_left, &mean_right))
                .collect::<Vec<f64>>())
        .collect::<Vec<f64>>();

    match Array::from_shape_vec((left_means.len(), right_means.len()), covariances) {
        Ok(array) => Ok(array.into_dyn()),
        Err(_) => Err("unable to form cross-covariance matrix".into())
    }
}

/// Get sample covariance between two 1D-arrays.
///
/// # Arguments
/// * `left` - One of the two arrays for which you want the covariance.
/// * `right` - One of the two arrays for which you want the covariance.
/// * `mean_left` - Arithmetic mean of the left array.
/// * `mean_right` - Arithmetic mean of the right array.
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
/// let cov = covariance(&left.view(), &right.view(), &mean_left, &mean_right);
/// assert!(cov == 1.);
/// ```
pub fn covariance(left: &ArrayView1<f64>, right: &ArrayView1<f64>, mean_left: &f64, mean_right: &f64) -> f64 {
    left.iter()
        .zip(right)
        .fold(0., |sum, (val_left, val_right)|
            sum + ((val_left - mean_left) * (val_right - mean_right))) / ( (left.len() - 1) as f64)
}