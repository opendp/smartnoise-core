use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, ReleaseNode};
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis, arr0};
use ndarray;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::get_argument;


impl Evaluable for proto::Count {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        Ok(ReleaseNode::new(match get_argument(arguments, "data")? {
            Value::Array(array) => match array {
                Array::Bool(data) => count(data)?.into(),
                Array::F64(data) => count(data)?.into(),
                Array::I64(data) => count(data)?.into(),
                Array::Str(data) => count(data)?.into()
            },
            Value::Indexmap(indexmap) => match indexmap.values().first() {
                Some(value) => arr0(value.array()?.num_records()?).into_dyn().into(),
                None => return Err("indexmap may not be empty".into())
            },
            Value::Jagged(_) => return Err("Count is not implemented on Jagged arrays".into()),
            Value::Function(_) => return Err("Count is not implemented on Functions".into())
        }))
    }
}

/// Gets number of rows of data.
///
/// # Arguments
/// * `data` - Data for which you want a count.
///
/// # Return
/// Number of rows in data.
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr1, arr2};
/// use whitenoise_runtime::components::count::count;
/// let data = arr2(&[ [false, false, true], [true, true, true] ]).into_dyn();
/// let n = count(&data).unwrap();
/// assert!(n.first().unwrap() == &2);
/// ```
pub fn count<T>(data: &ArrayD<T>) -> Result<ArrayD<i64>> {
    Ok(ndarray::Array::from_shape_vec(vec![], vec![data.len_of(Axis(0)) as i64])?)
}
