use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::{Evaluable};
use ndarray::{ArrayD, Array, Axis};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;
use crate::utilities::array::stack;

impl Evaluable for proto::Mean {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        match get_argument(&arguments, "data")? {
            Value::ArrayND(array) => Ok(mean(array.get_f64()?)?.into()),
            Value::Hashmap(hashmap) => {
                let aggregations = hashmap.get_values().iter()
                    .map(|value| mean(value.get_arraynd()?.get_f64()?))
                    .collect::<Result<Vec<ArrayD<f64>>>>()?;
                let views = aggregations.iter().map(|k| k.view()).collect();
                Ok(stack(Axis(0), &views)?.into())
            },
            _ => Err("Mean is only implemented for ArrayND and Hashmap".into())
        }
    }
}

pub fn mean(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {

    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.mean()).collect::<Option<Vec<f64>>>()
        .ok_or::<Error>("attempted mean of an empty column".into())?;

    let array = match data.ndim() {
        1 => Array::from_shape_vec(vec![], means),
        2 => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means),
        _ => return Err("invalid data shape for Mean".into())
    };

    match array {
        Ok(array) => Ok(array),
        Err(_) => Err("unable to package Mean result into an array".into())
    }
}
