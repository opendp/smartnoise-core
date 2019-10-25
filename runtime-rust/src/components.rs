use ndarray::prelude::*;
use crate::base::burdock;
use crate::base::{NodeArguments, NodeEvaluation, FieldEvaluation, parse_proto_array};
use crate::utilities;
extern crate num;

pub fn literal(x: &burdock::Literal) -> NodeEvaluation {
    println!("literal");
    let mut evaluation = NodeEvaluation::new();
    evaluation.insert("data".to_owned(), parse_proto_array(&x.to_owned().value.unwrap()));
    evaluation
}

pub fn datasource(x: &burdock::DataSource, dataset: &burdock::Dataset) -> NodeEvaluation {
    println!("datasource");

    // TODO: file parsing based on dataset proto paths
//    let mut evaluation = NodeEvaluation::new();
//    evaluation.insert("data".to_owned(), data.column(index).to_owned());
//    evaluation

    NodeEvaluation::new()
}

pub fn add(_x: &burdock::Add, arguments: &NodeArguments) -> NodeEvaluation {
    println!("add");
    let mut evaluation = NodeEvaluation::new();
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) => {
            evaluation.insert("data".to_owned(), FieldEvaluation::F64(x + y));
            Ok(0)
        },
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) => {
            evaluation.insert("data".to_owned(), FieldEvaluation::I64(x + y));
            Ok(0)
        }
        _ => Err("Add: Either the argument types are mismatched or non-nnumeric.")
    }.unwrap();
    evaluation
}

pub fn dp_mean_laplace(component: &burdock::DpMeanLaplace, arguments: &NodeArguments) -> NodeEvaluation {
    println!("dpmeanlaplace");

    let num_records = *match arguments.get("minumum").unwrap() {
        FieldEvaluation::I64(x) => Ok(x.first().unwrap()),
        _ => Err("num_records must be integer")
    }.unwrap() as f64;

    let epsilon = component.epsilon;

    let mut evaluation = NodeEvaluation::new();

    match arguments.get("data").unwrap() {
        FieldEvaluation::F64(data) => {
            // unpackage
            let minimum: f64 = *match arguments.get("minumum").unwrap() {
                FieldEvaluation::F64(x) => Ok(x.first().unwrap()),
                _ => Err("minimum must be float")
            }.unwrap();
            let maximum: f64 = *match arguments.get("maximum").unwrap() {
                FieldEvaluation::F64(x) => Ok(x.first().unwrap()),
                _ => Err("maximum must be float")
            }.unwrap();

            // computation
            let sensitivity = (maximum - minimum) / num_records;

            let mean = data.mapv(|v| num::clamp(v, minimum, maximum)).mean().unwrap();
            let noise = utilities::sample_laplace(0., sensitivity / epsilon);

            // repackage
            evaluation.insert("data".to_owned(),
                              FieldEvaluation::F64(Array::from_elem((), mean + noise).into_dyn()));
            Ok(evaluation)
        }

        FieldEvaluation::I64(data) => Err("Integer laplace mean has not been implemented."),
        _ => Err("Non-numeric laplace mean is invalid."),
    }.unwrap()
}