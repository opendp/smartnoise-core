use ndarray::prelude::*;
use crate::base::burdock;
use crate::base::{
    NodeArguments, NodeEvaluation, FieldEvaluation,
    parse_proto_array, get_f64, get_array_f64
};
use std::collections::HashMap;
extern crate csv;
use std::str::FromStr;
use crate::algorithms;
extern crate num;

macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

pub fn component_literal(x: &burdock::Literal) -> NodeEvaluation {
//    println!("literal");
    hashmap!["data".to_owned() => parse_proto_array(&x.to_owned().value.unwrap())]
}

pub fn component_datasource(datasource: &burdock::DataSource, dataset: &burdock::Dataset, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("datasource");

    let table = dataset.tables.get(&datasource.dataset_id).unwrap();
    let data = match table.value.as_ref().unwrap() {
        burdock::table::Value::FilePath(path) => {

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
                FieldEvaluation::Str(x) => Ok(match x.first().unwrap().as_ref() {
                    "BYTES" =>
                        Ok(FieldEvaluation::Bytes(Array1::from(get_column::<u8>(&path, &datasource.column_id)).into_dyn())),
                    "BOOL" =>
                        Ok(FieldEvaluation::Bool(Array1::from(get_column::<bool>(&path, &datasource.column_id)).into_dyn())),
                    "I64" =>
                        Ok(FieldEvaluation::I64(Array1::from(get_column::<i64>(&path, &datasource.column_id)).into_dyn())),
                    "F64" =>
                        Ok(FieldEvaluation::F64(Array1::from(get_column::<f64>(&path, &datasource.column_id)).into_dyn())),
                    "STRING" =>
                        Ok(FieldEvaluation::Str(Array1::from(get_column::<String>(&path, &datasource.column_id)).into_dyn())),
                    _ => Err("Datatype is not recognized.")
                }.unwrap()),
                _ => Err("Datatype must be a string.")
            }
        },
        burdock::table::Value::Literal(value) => Ok(parse_proto_array(&value)),
        _ => Err("Only file paths are supported")
    }.unwrap();

    hashmap!["data".to_owned() => data]
}

pub fn component_add(_x: &burdock::Add, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("add");
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x + y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x + y)]),
        _ => Err("Add: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap()
}


pub fn component_subtract(_x: &burdock::Subtract, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x - y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x - y)]),
        _ => Err("Subtract: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap()
}

pub fn component_divide(_x: &burdock::Divide, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x / y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x / y)]),
        _ => Err("Divide: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap()
}

pub fn component_multiply(_x: &burdock::Multiply, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x * y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x * y)]),
        _ => Err("Multiply: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap()
}

pub fn component_power(_x: &burdock::Power, arguments: &NodeArguments) -> NodeEvaluation {
    let power: f64 = get_f64(&arguments, "right");
    let data = get_array_f64(&arguments, "left");
    hashmap!["data".to_string() => FieldEvaluation::F64(data.mapv(|x| x.powf(power)))]
}

pub fn component_negate(_x: &burdock::Negate, arguments: &NodeArguments) -> NodeEvaluation {
    match arguments.get("data").unwrap() {
        FieldEvaluation::F64(x) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(-x)]),
        FieldEvaluation::I64(x) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(-x)]),
        _ => Err("Negate: Argument must be numeric.")
    }.unwrap()
}


pub fn component_dp_mean_laplace(component: &burdock::DpMeanLaplace, arguments: &NodeArguments) -> NodeEvaluation {
    // println!("dpmeanlaplace");
    hashmap!["data".to_string() => FieldEvaluation::F64(Array::from_elem((),algorithms::dp_mean_laplace(
        get_array_f64(&arguments, "data"),
        component.epsilon,
        get_f64(&arguments, "num_records"),
        get_f64(&arguments, "minimum"),
        get_f64(&arguments, "maximum")
    )).into_dyn())]
}
