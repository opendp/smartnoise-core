use ndarray::prelude::*;
use crate::base::burdock;
use crate::base::{NodeArguments, NodeEvaluation, FieldEvaluation, parse_proto_array};
use crate::utilities;
use std::collections::HashMap;
extern crate csv;
use std::str::FromStr;

extern crate num;

pub fn literal(x: &burdock::Literal) -> NodeEvaluation {
//    println!("literal");
    let mut evaluation = NodeEvaluation::new();
    evaluation.insert("data".to_owned(), parse_proto_array(&x.to_owned().value.unwrap()));
    evaluation
}

pub fn datasource(datasource: &burdock::DataSource, dataset: &burdock::Dataset) -> NodeEvaluation {
//    println!("datasource");
    let mut evaluation = NodeEvaluation::new();

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

            Ok(match burdock::DataType::from_i32(datasource.datatype).unwrap() {
                burdock::DataType::Bytes =>
                    FieldEvaluation::Bytes(Array1::from(get_column::<u8>(&path, &datasource.column_id)).into_dyn()),
                burdock::DataType::Bool =>
                    FieldEvaluation::Bool(Array1::from(get_column::<bool>(&path, &datasource.column_id)).into_dyn()),
                burdock::DataType::I64 =>
                    FieldEvaluation::I64(Array1::from(get_column::<i64>(&path, &datasource.column_id)).into_dyn()),
                burdock::DataType::F64 =>
                    FieldEvaluation::F64(Array1::from(get_column::<f64>(&path, &datasource.column_id)).into_dyn()),
                burdock::DataType::String =>
                    FieldEvaluation::Str(Array1::from(get_column::<String>(&path, &datasource.column_id)).into_dyn()),
            })
        },
        burdock::table::Value::Literal(value) => Ok(parse_proto_array(&value)),
        _ => Err("Only file paths are supported")
    }.unwrap();

    evaluation.insert("data".to_owned(), data);
    evaluation
}

pub fn add(_x: &burdock::Add, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("add");
    let mut evaluation = NodeEvaluation::new();
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) => {
            evaluation.insert("data".to_owned(), FieldEvaluation::F64(x + y));
            Ok(())
        },
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) => {
            evaluation.insert("data".to_owned(), FieldEvaluation::I64(x + y));
            Ok(())
        }
        _ => Err("Add: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap();
    evaluation
}

pub fn dp_mean_laplace(component: &burdock::DpMeanLaplace, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("dpmeanlaplace");

    // unpack
    let num_records = match arguments.get("num_records").unwrap() {
        FieldEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        FieldEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        FieldEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err("num_records must be numeric")
    }.unwrap();
    let minimum: f64 = match arguments.get("minimum").unwrap() {
        FieldEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        FieldEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        FieldEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err("minimum must be numeric")
    }.unwrap();
    let maximum: f64 = match arguments.get("maximum").unwrap() {
        FieldEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        FieldEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        FieldEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err("maximum must be numeric")
    }.unwrap();
    let data: ArrayD<f64> = match arguments.get("data").unwrap() {
        FieldEvaluation::Bool(x) => Ok(x.mapv(|v| if v {1.} else {0.})),
        FieldEvaluation::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
        FieldEvaluation::F64(x) => Ok(x.to_owned()),
        _ => Err("data must be numeric")
    }.unwrap();

    let epsilon = component.epsilon;

    // computation
    let sensitivity = (maximum - minimum) / num_records;

    let mean = data.mapv(|v| num::clamp(v, minimum, maximum)).mean().unwrap();
    let noise = utilities::sample_laplace(0., sensitivity / epsilon);

    let mut evaluation = NodeEvaluation::new();

    // repack
    evaluation.insert("data".to_owned(),
                      FieldEvaluation::F64(Array::from_elem((), mean + noise).into_dyn()));
    evaluation
}