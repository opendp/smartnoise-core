use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array, Axis, Array1, arr1};
use crate::utilities::utilities::get_num_columns;
use whitenoise_validator::proto;
use crate::components::resize::create_sampling_indices;
use crate::utilities::array::select;


impl Evaluable for proto::Filter {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let mask = get_argument(&arguments, "mask")?.get_arraynd()?.get_bool()?;

        Ok(match get_argument(&arguments, "data")?.get_arraynd()? {
            ArrayND::Str(data) => filter(data, mask)?.into(),
            ArrayND::F64(data) => filter(data, mask)?.into(),
            ArrayND::I64(data) => filter(data, mask)?.into(),
            ArrayND::Bool(data) => filter(data, mask)?.into(),
        })
    }
}


fn filter<T: Clone>(data: &ArrayD<T>, mask: &ArrayD<bool>) -> Result<ArrayD<T>> {

    let columnar_mask: Array1<bool> = mask.clone().into_dimensionality::<Ix1>().unwrap();

    let mask_indices: Vec<usize> = columnar_mask.iter().enumerate()
        .filter(|(index, &v)| v)
        .map(|(index, _)| index)
        .collect();
    Ok(select(&data, Axis(0), &mask_indices))
}