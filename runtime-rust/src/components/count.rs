use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::{Evaluable};
use ndarray::{ArrayD, Array, Axis, arr1, arr2};
use yarrow_validator::proto;
use crate::utilities::utilities::get_num_columns;
use crate::utilities::array::stack;


impl Evaluable for proto::Count {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::ArrayND(array) => Ok(count(array)?.into()),
            Value::Hashmap(hashmap) => {
                let num_columns = hashmap.get_values()[0].get_arraynd()?.get_num_columns()?;
                let aggregations = hashmap.get_values().iter()
                    .map(|value| Ok(value.get_arraynd()?.get_num_records()?))
                    .collect::<Result<Vec<i64>>>()?;
                Ok(arr1(&aggregations).broadcast((aggregations.len(), num_columns as usize)).unwrap().to_owned().into_dyn().into())
            },
            _ => Err("mean is only implemented for ArrayND and Hashmap".into())
        }
    }
}

pub fn count(data: &ArrayND) -> Result<ArrayD<i64>> {

    // iterate over the generalized columns. Of course, all columns will share the same length
    let count = data.get_num_records()?;
    let num_columns = data.get_num_columns()?;

    let array = match data.get_shape().len() {
        1 => Array::from_shape_vec(vec![], vec![count]),
        2 => Array::from_shape_vec(vec![1 as usize, num_columns as usize], (0..num_columns).map(|_| count).collect()),
        _ => return Err("invalid data shape for Count".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Count result into an array".into())
    }
}
