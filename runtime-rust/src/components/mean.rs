use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, Vector2DJagged, get_argument};
use crate::components::Evaluable;
use ndarray::{ArrayD, Axis, Array};
use crate::utilities::utilities::get_num_columns;
use yarrow_validator::proto;

impl Evaluable for proto::Mean {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

        match (arguments.get("by"), arguments.get("categories")) {
            (Some(by), Some(categories)) => match (by, categories) {
//                (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
//                    (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(mean_grouped(&data, &by, &categories)?))),
//                    (ArrayND::F64(by), Vector2DJagged::F64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(mean_grouped(&data, &by, &categories)?))),
//                    (ArrayND::I64(by), Vector2DJagged::I64(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(mean_grouped(&data, by, categories)?))),
//                    (ArrayND::Str(by), Vector2DJagged::Str(categories)) =>
//                        Ok(Value::Vector2DJagged(Vector2DJagged::F64(mean_grouped(&data, by, categories)?))),
//                    _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
//                }
                _ => return Err("by must be ArrayND and categories must be Vector2DJagged".into())
            }
            _ => Ok(Value::ArrayND(ArrayND::F64(mean(&data)?)))
        }
    }
}

pub fn mean(data: &ArrayD<f64>) -> Result<ArrayD<f64>> {
    let data = data.clone();

    // iterate over the generalized columns
    let means = data.gencolumns().into_iter()
        .map(|column| column.mean()).collect::<Option<Vec<f64>>>()
        .ok_or::<Error>("attempted mean of an empty column".into())?;

    // TODO: don't unwrap here
    Ok(match data.ndim() {
        1 => Array::from_shape_vec(vec![], means).unwrap(),
        _ => Array::from_shape_vec(vec![1 as usize, get_num_columns(&data)? as usize], means).unwrap()
    })
}

//pub fn mean_grouped<T,S>(data: &ArrayD<T>, by: &ArrayD<S>, categories: &Vec<Option<Vec<S>>>)
//                 -> Result<Vec<Option<Vec<f64>>>>
//    where T: Clone, T: Default, T: PartialEq, T: Add<T, Output=T>, T: std::convert::Into<f64>,
//          S: Clone, S: Default, S: PartialEq {
//
//    let mut counts: Vec<Option<Vec<i64>>> = count(by, categories).unwrap();
//    let mut sums: Vec<Option<Vec<T>>> = sum(data, by, categories).unwrap();
//    let mut means: Vec<Option<Vec<f64>>> = vec![Default::default(); sums.len()];
//
//    for i in 0..counts.len() {
//        let sum_vec: Option<Vec<T>> = sums[i].to_owned();
//        let count_vec: Option<Vec<i64>> = counts[i].to_owned();
//        means[i] = match (sum_vec, count_vec) {
//            (Some(sum), Some(mut count)) => {
//                let mut m: Vec<f64> = vec![Default::default(); count.len()];
//                let mut counter: i64 = 0;
//                for iter in sum.iter().zip(count.iter_mut()) {
//                    let (s, c) = iter;
//                    m[i] = (s.clone().into()) / (*c as f64);
//                    counter = counter + 1;
//                }
//                Some(m)
//            },
//            (None, None) => None,
//            _ => return Err("implicit sums and counts must have Some and None in same indices".into())
//        };
//    }
//
//    return Ok(means)
//}
