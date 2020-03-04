use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, Vector2DJagged, standardize_numeric_argument, standardize_categorical_argument, standardize_null_argument};
use crate::components::Evaluable;
use ndarray::ArrayD;
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;

impl Evaluable for proto::Clamp {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        if arguments.contains_key("categories") {
            match (arguments.get("data").unwrap(), arguments.get("categories").unwrap(), arguments.get("null").unwrap()) {
                (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(nulls)) => Ok(match (data, categories, nulls) {
                    (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::Bool(nulls)) =>
                        Value::ArrayND(ArrayND::Bool(clamp_categorical(&data, &categories, &nulls)?)),
                    (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(nulls)) =>
                        Value::ArrayND(ArrayND::F64(clamp_categorical(&data, &categories, &nulls)?)),
                    (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::I64(nulls)) =>
                        Value::ArrayND(ArrayND::I64(clamp_categorical(&data, &categories, &nulls)?)),
                    (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::Str(nulls)) =>
                        Value::ArrayND(ArrayND::Str(clamp_categorical(&data, &categories, &nulls)?)),
                    _ => return Err("types of data, categories, and null must be consistent".into())
                }),
                _ => return Err("data must be ArrayND, categories must be Vector2DJagged, and null must be ArrayND".into())
            }
        } else {
            match (arguments.get("data").unwrap(), arguments.get("min").unwrap(), arguments.get("max").unwrap()) {
                (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => Ok(match (data, min, max) {
                    (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                        Value::ArrayND(ArrayND::F64(clamp_numeric_float(&data, &min, &max)?)),
//                    (ArrayND::I64(data), ArrayND::I64(min), ArrayND::I64(max)) =>
//                        Value::ArrayND(ArrayND::I64(clamp_numeric_integer(data, min, max)?)),
                    _ => return Err("data, min, and max must all have type f64".into())
                }),
                _ => return Err("data, min, and max must all be ArrayND".into())
            }
        }
    }
}

/// Clamps each column of numeric data to [min, max]
///
/// # Example
/// ```
/// use ndarray::{ArrayD, arr2, arr1};
/// use yarrow_runtime::utilities::transformations::{convert_to_matrix, clamp_numeric};
/// let data = arr2(&[ [1.,2.,3.], [7.,11.,9.] ]).into_dyn();
/// let mut data_2d: ArrayD<f64> = convert_to_matrix(&data);
/// let mins: ArrayD<f64> = arr1(&[0.5,8.]).into_dyn();
/// let maxes: ArrayD<f64> = arr1(&[2.5,10.]).into_dyn();
/// let mut clamped_data = clamp_numeric(&data_2d, &mins, &maxes);
/// println!("{:?}", data_2d);
/// println!("{:?}", clamped_data);
/// ```
pub fn clamp_numeric_float(
    data: &ArrayD<f64>, min: &ArrayD<f64>, max: &ArrayD<f64>
)-> Result<ArrayD<f64>> {
    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_numeric_argument(&min, &num_columns)?.iter())
        .zip(standardize_numeric_argument(&max, &num_columns)?.iter())
        // for each pairing, iterate over the cells
        .for_each(|((mut column, min), max)| column.iter_mut()
            // ignore nan values
            .filter(|v| !v.is_nan())
            // mutate the cell via the operator
            .for_each(|v| *v = min.max(max.min(v.clone()))));

    Ok(data)
}

pub fn clamp_categorical<T>(data: &ArrayD<T>, categories: &Vec<Option<Vec<T>>>, null_value: &Vec<Option<Vec<T>>>)
                            -> Result<ArrayD<T>> where T:Clone, T:PartialEq, T:Default {

    let mut data = data.clone();

    let num_columns = get_num_columns(&data)?;

    // iterate over the generalized columns
    data.gencolumns_mut().into_iter()
        // pair generalized columns with arguments
        .zip(standardize_categorical_argument(&categories, &num_columns)?)
        .zip(standardize_null_argument(&null_value, &num_columns)?)
        // for each pairing, iterate over the cells
        .for_each(|((mut column, categories), null)| column.iter_mut()
            // ignore known values
            .filter(|v| !categories.contains(v))
            // mutate the cell via the operator
            .for_each(|v| *v = null.clone()));

    Ok(data)
}

