use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use whitenoise_validator::utilities::get_argument;
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis, Array1};

use whitenoise_validator::proto;

use whitenoise_validator::utilities::array::slow_select;
use crate::utilities::to_nd;


impl Evaluable for proto::Filter {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mask = get_argument(arguments, "mask")?.array()?.bool()?;

        Ok(ReleaseNode::new(match get_argument(arguments, "data")?.array()? {
            Array::Str(data) => filter(data, mask)?.into(),
            Array::F64(data) => filter(data, mask)?.into(),
            Array::I64(data) => filter(data, mask)?.into(),
            Array::Bool(data) => filter(data, mask)?.into(),
        }))
    }
}

/// Filters data down into only the desired rows.
///
/// # Arguments
/// * `data` - Data to be filtered.
/// * `mask` - Boolean mask giving whether or not each row should be kept.
///
/// # Return
/// Data with only the desired rows.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::filter::filter;
///
/// let data = arr2(&[ [1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12] ]).into_dyn();
/// let mask = arr1(&[true, false, true, false]).into_dyn();
/// let filtered = filter(&data, &mask).unwrap();
/// assert!(filtered == arr2(&[ [1, 2, 3], [7, 8, 9] ]).into_dyn());
/// ```
pub fn filter<T: Clone + Default>(data: &ArrayD<T>, mask: &ArrayD<bool>) -> Result<ArrayD<T>> {

    let columnar_mask: Array1<bool> = to_nd(mask.clone(), &1)?.into_dimensionality::<Ix1>()?;

    let mask_indices: Vec<usize> = columnar_mask.iter().enumerate()
        .filter(|(_index, &v)| v)
        .map(|(index, _)| index)
        .collect();
    Ok(slow_select(&data, Axis(0), &mask_indices))
}