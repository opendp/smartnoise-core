use yarrow_validator::errors::*;

use ndarray::prelude::*;
use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Array, Axis, Array1, arr1};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;
use crate::components::resize::create_sampling_indices;
use crate::utilities::array::select;


impl Evaluable for proto::Filter {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?;
        let mask = get_argument(&arguments, "mask")?.get_arraynd()?.get_bool()?;

        Ok(match data {
            ArrayND::Str(data) => Value::ArrayND(ArrayND::Str(filter(data, mask)?)),
            ArrayND::F64(data) => Value::ArrayND(ArrayND::F64(filter(data, mask)?)),
            ArrayND::I64(data) => Value::ArrayND(ArrayND::I64(filter(data, mask)?)),
            ArrayND::Bool(data) => Value::ArrayND(ArrayND::Bool(filter(data, mask)?)),
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