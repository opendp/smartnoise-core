use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::Value;
use crate::components::Evaluable;
use yarrow_validator::proto;


impl Evaluable for proto::Sum {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let data = match arguments.get("data").unwrap() {
            Value::ArrayND(data) => data,
            _ => return Err("data must be an ArrayND".into())
        };

        match (arguments.get("by").unwrap(), arguments.get("categories").unwrap()) {
            (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
                (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) => match(data) {
                    ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::sum(&data, by, categories)?))),
                    ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::sum(&data, by, categories)?))),
                    _ => return Err("data must be either f64 or i64".into())
                }
                (ArrayND::F64(by), Vector2DJagged::F64(categories)) => match(data) {
                    ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::sum(&data, by, categories)?))),
                    ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::sum(&data, by, categories)?))),
                    _ => return Err("data must be either f64 or i64".into())
                }
                (ArrayND::I64(by), Vector2DJagged::I64(categories)) => match(data) {
                    ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::sum(&data, by, categories)?))),
                    ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::sum(&data, by, categories)?))),
                    _ => return Err("data must be either f64 or i64".into())
                }
                (ArrayND::Str(by), Vector2DJagged::Str(categories)) => match(data) {
                    ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::sum(&data, by, categories)?))),
                    ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::sum(&data, by, categories)?))),
                    _ => return Err("data must be either f64 or i64".into())
                }
                _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
            }
            _ => return Err("by must be ArrayND and categories must be Vector2DJagged".into())
        }
    }
}

pub fn sum<T,S>(data: &ArrayD<T>, by: &ArrayD<S>, categories: &Vec<Option<Vec<S>>>)
                -> Result<Vec<Option<Vec<T>>>>
    where T: Clone, T: Default, T: PartialEq, T: Add<T, Output=T>,
          S: Clone, S: Default, S: PartialEq {

    let mut data_2d: ArrayD<T> = convert_to_matrix(data);
    let mut by_2d: ArrayD<S> = convert_to_matrix(by);
    let mut sums: Vec<Option<Vec<T>>> = Vec::with_capacity(categories.len());

    let n_cols: i64 = data_2d.len_of(Axis(0)) as i64;

    for i in 0..n_cols {
        let mut data_vec: Vec<T> = data_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
            into_dimensionality::<Ix1>().unwrap().to_vec();
        let mut by_vec: Vec<S> = by_2d.slice(s![i as usize, ..]).clone().into_dyn().clone().
            into_dimensionality::<Ix1>().unwrap().to_vec();
        let category_vec: Vec<S> = categories[i as usize].clone().unwrap();
        let mut sum_vec: Vec<T> = vec![Default::default(); category_vec.len()];

        for j in 0..by_vec.len() {
            for k in 0..category_vec.len() {
                if by_vec[j as usize] == category_vec[k as usize] {
                    sum_vec[k] = sum_vec[k as usize].clone() + data_vec[j as usize].clone();
                }
            }
        }

        sums.push(Some(sum_vec))
    }
    return Ok(sums)
}