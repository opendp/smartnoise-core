extern crate yarrow_validator;
use yarrow_validator::errors::*;
use yarrow_validator::ErrorKind::{PrivateError, PublicError};

use yarrow_validator::proto;
use crate::utilities;

use std::convert::TryFrom;
use ndarray::prelude::*;
use std::collections::HashMap;

extern crate csv;
extern crate num;

use std::str::FromStr;
use yarrow_validator::base::{get_argument, Value, ArrayND, Vector2DJagged};

use yarrow_validator::utilities::serial::{parse_value};
use crate::base::NodeArguments;

pub fn component_constant(x: &proto::Constant) -> Result<Value> {
    parse_value(&x.to_owned().value.unwrap())
}

pub fn component_materialize(
    materialize: &proto::Materialize,
    dataset: &proto::Dataset,
) -> Result<Value> {
    let table = dataset.tables.get(&materialize.dataset_id).unwrap();
    match table.value.as_ref().unwrap() {
        proto::table::Value::Literal(value) => parse_value(value),
        proto::table::Value::FilePath(path) => {
            let mut response = HashMap::<String, Vec<String>>::new();
            csv::Reader::from_path(path).unwrap().deserialize()
                .for_each(|result| {
                    // parse each record into the yarrow internal format
                    let record: HashMap<String, String> = result.unwrap();
                    record.iter().for_each(|(k, v)| response
                        .entry(k.to_owned()).or_insert_with(Vec::new)
                        .push(v.clone()));
                });
            Ok(Value::HashmapString(response.iter()
                .map(|(k, v): (&String, &Vec<String>)| (
                    k.clone(), Value::ArrayND(ArrayND::Str(Array::from(v.to_owned()).into_dyn()))
                ))
                .collect::<HashMap<String, Value>>()))
        }
        _ => Err("the selected table reference format is not implemented".into())
    }
}

pub fn component_index(_index: &proto::Index, arguments: &NodeArguments) -> Result<Value> {
    let data = arguments.get("data").unwrap();
    let columns = arguments.get("columns").unwrap();

    match data {
        Value::HashmapString(dataframe) => match columns {
            Value::ArrayND(array) => match array {
                ArrayND::Str(column_names) => match column_names.ndim() {
                    0 => Ok(dataframe.get(column_names.first().unwrap()).unwrap().to_owned()),
//                1 => match column_names.into_dimensionality::<Ix1>() {
//                    Ok(column_names) =>
//                        Value::Str(stack(Axis(0), column_names.to_vec().iter()
//                            .map(|column_name| match dataframe.get(column_names.first().unwrap()).unwrap() {
//                                Value::Str(array) => array,
//                                _ => panic!("selected data frame columns are not of a homogenous type".to_string())
//                            }).collect()).unwrap())
//                            .collect::<Vec<ArrayD<str>>>(),
//                    _ => Err("column names must be at most 1-dimensional".to_owned()),
//                },
                    _ => Err("column names must be at most 1-dimensional".into())
                },
                _ => Err("column names must be strings".into())
            },
            _ => Err("column names must an array".into())
        },
        _ => Err("indexing is only implemented for hashmaps".into())
    }
}

pub fn component_datasource(
    datasource: &proto::DataSource, dataset: &proto::Dataset, arguments: &NodeArguments,
) -> Result<Value> {
//    println!("datasource");

    let table = dataset.tables.get(&datasource.dataset_id).unwrap();
    Ok(match table.value.as_ref().unwrap() {
        proto::table::Value::FilePath(path) => {
            fn get_column<T>(path: &String, column: &String) -> Vec<T>
                where T: FromStr, <T as std::str::FromStr>::Err: std::fmt::Debug {
                let mut rdr = csv::Reader::from_path(path).unwrap();
                rdr.deserialize().map(|result| {
                    let record: HashMap<String, String> = result.unwrap();
//                    println!("{:?}", record);
                    record[column].parse::<T>().unwrap()
                }).collect()
            }

            match arguments.get("datatype").unwrap() {
                Value::ArrayND(array) => match array {
                    ArrayND::Str(x) => match x.first().unwrap().as_ref() {
//                    "BYTES" =>
//                        Ok(Value::Bytes(Array1::from(get_column::<u8>(&path, &datasource.column_id)).into_dyn())),
                        "BOOL" =>
                            Value::ArrayND(ArrayND::Bool(Array1::from(get_column::<bool>(&path, &datasource.column_id)).into_dyn())),
                        "I64" =>
                            Value::ArrayND(ArrayND::I64(Array1::from(get_column::<i64>(&path, &datasource.column_id)).into_dyn())),
                        "F64" =>
                            Value::ArrayND(ArrayND::F64(Array1::from(get_column::<f64>(&path, &datasource.column_id)).into_dyn())),
                        "STRING" =>
                            Value::ArrayND(ArrayND::Str(Array1::from(get_column::<String>(&path, &datasource.column_id)).into_dyn())),
                        _ => return Err("Datatype is not recognized.".into())
                    },
                    _ => return Err("Datatype must be a string.".into())
                }
                _ => return Err("Datatype must be contained in an array.".into())
            }
        }
        proto::table::Value::Literal(value) => parse_value(&value)?,
        _ => return Err("Only file paths are supported".into())
    })
}

pub fn component_add(
    _x: &proto::Add, arguments: &NodeArguments,
) -> Result<Value> {
//    println!("add");
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(x + y))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(x + y))),
            _ => Err("Add: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Add: Both arguments must be arrays.".into())
    }
}


pub fn component_subtract(
    _x: &proto::Subtract, arguments: &NodeArguments,
) -> Result<Value> {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(x - y))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(x - y))),
            _ => Err("Subtract: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Subtract: Both arguments must be arrays.".into())
    }
}

pub fn component_divide(
    _x: &proto::Divide, arguments: &NodeArguments,
) -> Result<Value> {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(x / y))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(x / y))),
            _ => Err("Divide: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Divide: Both arguments must be arrays.".into())
    }
}

pub fn component_multiply(
    _x: &proto::Multiply, arguments: &NodeArguments,
) -> Result<Value> {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(x * y))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(x * y))),
            _ => Err("Multiply: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Multiply: Both arguments must be arrays.".into())
    }
}

pub fn component_power(
    _x: &proto::Power, arguments: &NodeArguments,
) -> Result<Value> {
    let power: f64 = get_argument(&arguments, "right")?.get_first_f64()?;
    let data = get_argument(&arguments, "right")?.get_arraynd()?.get_f64()?;
    Ok(Value::ArrayND(ArrayND::F64(data.mapv(|x| x.powf(power)))))
}

pub fn component_negate(
    _x: &proto::Negate, arguments: &NodeArguments,
) -> Result<Value> {
    match arguments.get("data").unwrap() {
        Value::ArrayND(data) => match data {
            ArrayND::F64(x) =>
                Ok(Value::ArrayND(ArrayND::F64(-x))),
            ArrayND::I64(x) =>
                Ok(Value::ArrayND(ArrayND::I64(-x))),
            _ => Err("Negate: Argument must be numeric.".into())
        },
        _ => Err("Negate: Argument must be an array.".into())
    }
}

pub fn component_bin(
    _x: &proto::Bin, arguments: &NodeArguments,
) -> Result<Value> {
    let inclusive_left: ArrayD<bool> = get_argument(&arguments, "inclusive_left")?.get_arraynd()?.get_bool()?;

    let data = get_argument(&arguments, "data")?.get_arraynd()?;
    let edges = get_argument(&arguments, "edges")?.get_arraynd()?;

    match (data, edges) {
        (ArrayND::F64(data), ArrayND::F64(edges)) => Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::bin(&data, &edges, &inclusive_left)?))),
        (ArrayND::I64(data), ArrayND::I64(edges)) => Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::bin(&data, &edges, &inclusive_left)?))),
        _ => return Err("data and edges must both be f64 or i64".into())
    }
}

pub fn component_count(_x: &proto::Count, arguments: &NodeArguments,) -> Result<Value> {
    match (arguments.get("data").unwrap(), arguments.get("categories").unwrap()) {
        (Value::ArrayND(data), Value::Vector2DJagged(categories)) => match (data, categories) {
            (ArrayND::Bool(data), Vector2DJagged::Bool(categories)) =>
                Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::count(&data, categories)?))),

            (ArrayND::F64(data), Vector2DJagged::F64(categories)) =>
                Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::count(&data, categories)?))),

            (ArrayND::I64(data), Vector2DJagged::I64(categories)) =>
                Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::count(&data, categories)?))),

            (ArrayND::Str(data), Vector2DJagged::Str(categories)) =>
                Ok(Value::Vector2DJagged(Vector2DJagged::I64(utilities::transformations::count(&data, categories)?))),
            _ => return Err("data and categories must be of same atomic type".into())
        }
        _ => return Err("data must be ArrayND and categories must be Vector2dJagged".into())
    }
}

pub fn component_sum(_x: &proto::Sum, arguments: &NodeArguments,) -> Result<Value> {
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

// TODO: may want to accept i64 as input, but need to find way to convert to f64 at some point (talk to Mike)
pub fn component_mean(_x: &proto::Mean, arguments: &NodeArguments,) -> Result<Value> {
    let data = match arguments.get("data").unwrap() {
        Value::ArrayND(data) => data,
        _ => return Err("data must be an ArrayND".into())
    };

    match (arguments.get("by").unwrap(), arguments.get("categories").unwrap()) {
        (Value::ArrayND(by), Value::Vector2DJagged(categories)) => match (by, categories) {
            (ArrayND::Bool(by), Vector2DJagged::Bool(categories)) => match(data) {
                // ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                _ => return Err("data must be f64".into())
            }
            (ArrayND::F64(by), Vector2DJagged::F64(categories)) => match(data) {
                // ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                _ => return Err("data must be f64".into())
            }
            (ArrayND::I64(by), Vector2DJagged::I64(categories)) => match(data) {
                // ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                _ => return Err("data must be f64".into())
            }
            (ArrayND::Str(by), Vector2DJagged::Str(categories)) => match(data) {
                // ArrayND::I64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                ArrayND::F64(data) => Ok(Value::Vector2DJagged(Vector2DJagged::F64(utilities::transformations::mean(&data, by, categories)?))),
                _ => return Err("data must be f64".into())
            }
        _ => return Err("data and by must be ArrayND and categories must be Vector2dJagged".into())
        }
        _ => return Err("by must be ArrayND and categories must be Vector2DJagged".into())
    }
}

pub fn component_row_wise_min(
    _x: &proto::RowMin, arguments: &NodeArguments,
     ) -> Result<Value> {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::broadcast_map(
                    &x, &y, &|l: &f64, r: &f64| l.min(*r))?))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::broadcast_map(
                    &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?))),
            _ => Err("Min: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Min: Both arguments must be arrays.".into())
    }
}

pub fn component_row_wise_max(
    _x: &proto::RowMax, arguments: &NodeArguments,
     ) -> Result<Value> {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (Value::ArrayND(left), Value::ArrayND(right)) => match (left, right) {
            (ArrayND::F64(x), ArrayND::F64(y)) =>
                Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::broadcast_map(
                    &x, &y, &|l: &f64, r: &f64| l.max(*r))?))),
            (ArrayND::I64(x), ArrayND::I64(y)) =>
                Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::broadcast_map(
                    &x, &y, &|l: &i64, r: &i64| *std::cmp::max(l, r))?))),
            _ => Err("Max: Either the argument types are mismatched or non-numeric.".into())
        },
        _ => Err("Max: Both arguments must be arrays.".into())
    }
}

pub fn component_clamp(_x: &proto::Clamp, arguments: &NodeArguments) -> Result<Value> {
    println!("COMPONENT CLAMP {:?}", arguments);
    if arguments.contains_key("categories") {
        match (arguments.get("data").unwrap(), arguments.get("categories").unwrap(), arguments.get("null").unwrap()) {
            (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(nulls)) => match (data, categories, nulls) {
                (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::Bool(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<bool>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<bool>>>();
                        return Ok(Value::ArrayND(ArrayND::Bool(utilities::transformations::clamp_categorical(&data, &categories, &nulls)?)));
                    },
                (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        return Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::clamp_categorical(&data, &categories, &nulls)?)));
                    },
                (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::I64(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<i64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<i64>>>();
                        return Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::clamp_categorical(&data, &categories, &nulls)?)));
                    },
                (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::Str(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<String>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<String>>>();
                        return Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::clamp_categorical(&data, &categories, &nulls)?)));
                    },
                _ => return Err("types of data, categories, and null must be consistent".into())
            },
            _ => return Err("data must be ArrayND, categories must be Vector2DJagged, and null must be ArrayND".into())
        }
    } else {
        match (arguments.get("data").unwrap(), arguments.get("min").unwrap(), arguments.get("max").unwrap()) {
            (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => match (data, min, max) {
                (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                    return Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::clamp_numeric(&data, &min, &max)))),
                (ArrayND::I64(data), ArrayND::I64(min), ArrayND::I64(max)) =>
                    return Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::clamp_numeric(&data, &min, &max)))),
                _ =>
                    return Err("data, min, and max must all have type f64 or i64".into())
            },
            _ => return Err("data, min, and max must all be ArrayND".into())
        }
    }
}

// TODO: still working on this
pub fn component_impute(_x: &proto::Impute, arguments: &NodeArguments,) -> Result<Value> {
    let uniform: String = "Uniform".to_string(); // Distributions
    let gaussian: String = "Gaussian".to_string();

    if arguments.contains_key("categories") {
        match (arguments.get("data").unwrap(), arguments.get("categories").unwrap(), arguments.get("probabilities").unwrap(), arguments.get("null").unwrap()) {
            (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities), Value::Vector2DJagged(nulls)) => match (data, categories, probabilities, nulls) {
                (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Bool(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<bool>>>();
                        // let probabilities = probabilities.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<bool>>>();
                        return Ok(Value::ArrayND(ArrayND::Bool(utilities::transformations::impute_categorical(&data, &categories, &probabilities, &nulls)?)));
                    },
                (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::F64(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let probabilities = probabilities.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        return Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::impute_categorical(&data, &categories, &probabilities, &nulls)?)));
                    },
                (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::I64(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<i64>>>();
                        // let probabilities = probabilities.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<i64>>>();
                        return Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::impute_categorical(&data, &categories, &probabilities, &nulls)?)));
                    },
                (ArrayND::Str(data), Vector2DJagged::Str(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Str(nulls)) =>
                    {
                        // let categories = categories.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<String>>>();
                        // let probabilities = probabilities.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                        // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<String>>>();
                        return Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::impute_categorical(&data, &categories, &probabilities, &nulls)?)));
                    },
                _ => return Err("types of data, categories, and null must be consistent and probabilities must be f64".into())
            },
            _ => return Err("data and null must be ArrayND, categories and probabilities must be Vector2DJagged".into())
        }
    } else {
        let distribution = match arguments.get("distribution") {
            Some(distribution) => match distribution {
                Value::ArrayND(array) => match array {
                    ArrayND::Str(distribution) => distribution.first().unwrap().to_owned(),
                    _ => return Err("distribution must be a string".into())
                },
                _ => return Err("distribution must be wrapped in an ArrayND".into())
            },
            None => "Uniform".to_string()
        };

        match &distribution.clone() {
            x if x == &uniform => {
                match (arguments.get("data").unwrap(), arguments.get("min").unwrap(), arguments.get("max").unwrap()) {
                    (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max))
                        => match (data, min, max) {
                            (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max))
                                => return Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::impute_numeric(
                                             &data, &distribution, &min, &max, &None, &None)))),
                            (ArrayND::I64(data), ArrayND::I64(_min), ArrayND::I64(_max))
                                => return Ok(Value::ArrayND(ArrayND::I64(data.clone()))),
                            _ => return Err("data, min, and max must all be the same type".into())
                        }
                    _ => return Err("data, min, max, shift, and scale must be ArrayND".into())
                }
            },
            x if x == &gaussian => {
                match (arguments.get("data").unwrap(), arguments.get("min").unwrap(),
                       arguments.get("max").unwrap(), arguments.get("shift").unwrap(), arguments.get("scale").unwrap()) {
                    (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max), Value::ArrayND(shift), Value::ArrayND(scale))
                        => match(data, min, max, shift, scale) {
                            (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max), ArrayND::F64(shift), ArrayND::F64(scale))
                                => return Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::impute_numeric(
                                             &data, &distribution,  &min, &max, &Some(shift.clone()), &Some(scale.clone()))))),
                            _ => return Err("data, min, max, shift, and scale must all be f64".into())
                        },
                    _ =>
                        return Err("data, min, max, shift, and scale must all be ArrayND".into())
                };
            },
            _ => return Err("Distribution not supported".into())
        }
    }
}


pub fn component_resize(_x: &proto::Resize, arguments: &NodeArguments) -> Result<Value> {
    let distribution = match get_argument(&arguments, "distribution")?.get_first_str() {
        Ok(distribution) => distribution.to_string(),
        Err(_) => "Uniform".to_string()
    };
    let n = u64::try_from(get_argument(&arguments, "n")?.get_first_i64()?).unwrap();

    if arguments.contains_key("categories") {

        // // TODO: refactor into separate function
        // let probabilities = match arguments.get("probabilities") {
        //     Some(probabilities) => match probabilities {
        //         Value::Vector2DJagged(probabilities) => match probabilities {
        //             Vector2DJagged::F64(probabilities) =>
        //                 probabilities.iter().map(|prob| prob.to_owned().unwrap()).collect(),
        //             _ => return Err("probability vectors must be floats".into())
        //         }
        //         _ => return Err("probability vectors must be contained within jagged matrices".into())
        //     },
        //     // TODO: infer uniform probability
        //     None => return Err("probability vectors must be supplied as an argument".into())
        // };

        match (arguments.get("data").unwrap(), arguments.get("categories").unwrap(),
               arguments.get("probabilities").unwrap(), arguments.get("null").unwrap()) {
            (Value::ArrayND(data), Value::Vector2DJagged(categories), Value::Vector2DJagged(probabilities), Value::Vector2DJagged(nulls))
                => match (data, categories, probabilities, nulls) {

                (ArrayND::F64(data), Vector2DJagged::F64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::F64(nulls)) => {
                    // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<f64>>>();
                    Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::resize_categorical(&data, &n, &categories, &probabilities, &nulls))))
                },

                (ArrayND::I64(data), Vector2DJagged::I64(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::I64(nulls)) => {
                    // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<i64>>>();
                    Ok(Value::ArrayND(ArrayND::I64(utilities::transformations::resize_categorical(&data, &n, &categories, &probabilities, &nulls))))
                }

                (ArrayND::Bool(data), Vector2DJagged::Bool(categories), Vector2DJagged::F64(probabilities), Vector2DJagged::Bool(nulls)) => {
                    // let nulls = nulls.iter().map(|column| column.to_owned().unwrap()).collect::<Vec<Vec<bool>>>();
                    Ok(Value::ArrayND(ArrayND::Bool(utilities::transformations::resize_categorical(&data, &n, &categories, &probabilities, &nulls))))
                }
//                (ArrayND::Str(data), Vector2DJagged::Str(categories), ArrayND::Str(nulls)) =>
//                    Ok(Value::ArrayND(ArrayND::Str(utilities::transformations::resize_categorical(&data, &n, &unwrap_jagged(&categories), &probabilities, &nulls)))),
                _ => Err("types of data, categories and nulls must be homogenous, probabilities must be f64".into())
            },
            _ => Err("data and nulls must be arrays, categories must be a jagged matrix".into())
        }
    } else {
        let shift = get_argument(&arguments, "shift")?.get_arraynd()?.get_f64();
        let scale = get_argument(&arguments, "scale")?.get_arraynd()?.get_f64();
        match (arguments.get("data").unwrap(), arguments.get("min").unwrap(), arguments.get("max").unwrap()) {
            (Value::ArrayND(data), Value::ArrayND(min), Value::ArrayND(max)) => match (data, min, max) {
                (ArrayND::F64(data), ArrayND::F64(min), ArrayND::F64(max)) =>
                    Ok(Value::ArrayND(ArrayND::F64(utilities::transformations::resize_numeric(&data, &n, &distribution, &min, &max, &shift.ok(), &scale.ok())))),
                _ => Err("data, min and max must all be of float type".into())
            },
            _ => Err("data, min and max must all be arrays".into())
        }
    }
}

//pub fn component_count(
//    _X: &proto::Count, arguments: &NodeArguments,
//) -> Result<Value> {
//
//    match (arguments.get("data").unwrap(), arguments.get("group_by").unwrap()) {
//        (Value::F64(data), Value::F64(group_by)) =>
//            Ok(Value::F64(utilities::aggregations::count(&get_array_f64(&arguments, "data")?, &Some(get_array_f64(&arguments, "group_by")?)))),
//        (Value::Str(data), Value::Str(group_by)) =>
//            Ok(Value::F64(utilities::aggregations::count(&get_array_str(&arguments, "data")?, &Some(get_array_str(&arguments, "group_by")?)))),
//        (Value::Bool(data), Value::Bool(group_by)) =>
//            Ok(Value::F64(utilities::aggregations::count(&get_array_bool(&arguments, "data")?, &Some(get_array_bool(&arguments, "group_by")?)))),
//        _ => Err("Count: Data type must be f64, string, or bool".into())
//    }
//}

// pub fn component_mean(
//     _x: &proto::Mean, arguments: &NodeArguments,
// ) -> Result<Value> {
//     let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
//     Ok(Value::ArrayND(ArrayND::F64(utilities::aggregations::mean(&data))))
// }

pub fn component_variance(
    _x: &proto::Variance, arguments: &NodeArguments,
) -> Result<Value> {
    let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
    let finite_sample_correction = get_argument(&arguments, "shift")?.get_first_bool()?;
    Ok(Value::ArrayND(ArrayND::F64(utilities::aggregations::variance(&data, &finite_sample_correction))))
}

pub fn component_kth_raw_sample_moment(
    _x: &proto::KthRawSampleMoment, arguments: &NodeArguments,
) -> Result<Value> {
    let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
    let k = get_argument(&arguments, "k")?.get_first_i64()?;
    Ok(Value::ArrayND(ArrayND::F64(utilities::aggregations::kth_raw_sample_moment(&data, &k))))
}

pub fn component_median(
    _x: &proto::Median, arguments: &NodeArguments,
) -> Result<Value> {
    let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;
    Ok(Value::ArrayND(ArrayND::F64(utilities::aggregations::median(&data))))
}

pub fn component_laplace_mechanism(
    _x: &proto::LaplaceMechanism, arguments: &NodeArguments,
) -> Result<Value> {
    let epsilon = get_argument(&arguments, "epsilon")?.get_first_f64()?;
    let sensitivity = get_argument(&arguments, "sensitivity")?.get_first_f64()?;
    Ok(Value::ArrayND(ArrayND::F64(utilities::mechanisms::laplace_mechanism(&epsilon, &sensitivity))))
}

pub fn component_gaussian_mechanism(
    _x: &proto::GaussianMechanism, arguments: &NodeArguments,
) -> Result<Value> {
    let epsilon = get_argument(&arguments, "epsilon")?.get_first_f64()?;
    let delta = get_argument(&arguments, "delta")?.get_first_f64()?;
    let sensitivity = get_argument(&arguments, "sensitivity")?.get_first_f64()?;
    Ok(Value::ArrayND(ArrayND::F64(utilities::mechanisms::gaussian_mechanism(&epsilon, &delta, &sensitivity))))
}

pub fn component_simple_geometric_mechanism(
    _x: &proto::SimpleGeometricMechanism, arguments: &NodeArguments,
) -> Result<Value> {
    let epsilon = get_argument(&arguments, "epsilon")?.get_first_f64()?;
    let sensitivity = get_argument(&arguments, "sensitivity")?.get_first_f64()?;
    let count_min = get_argument(&arguments, "count_min")?.get_first_i64()?;
    let count_max = get_argument(&arguments, "count_max")?.get_first_i64()?;
    let enforce_constant_time = get_argument(&arguments, "enforce_constant_time")?.get_first_bool()?;

    Ok(Value::ArrayND(ArrayND::I64(utilities::mechanisms::simple_geometric_mechanism(
        &epsilon, &sensitivity, &count_min, &count_max, &enforce_constant_time))))
}
