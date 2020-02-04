extern crate yarrow_validator;
use yarrow_validator::yarrow;

use ndarray::prelude::*;
use crate::base::*;
use std::collections::HashMap;
extern crate csv;
use std::str::FromStr;
use crate::algorithms;
use crate::utilities;

extern crate num;

macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

pub fn component_literal(x: &yarrow::Literal) -> NodeEvaluation {
//    println!("literal");
    hashmap!["data".to_owned() => parse_proto_array(&x.to_owned().value.unwrap())]
}

pub fn component_datasource(datasource: &yarrow::DataSource, dataset: &yarrow::Dataset, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("datasource");

    let table = dataset.tables.get(&datasource.dataset_id).unwrap();
    let data = match table.value.as_ref().unwrap() {
        yarrow::table::Value::FilePath(path) => {

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
        yarrow::table::Value::Literal(value) => Ok(parse_proto_array(&value)),
        _ => Err("Only file paths are supported")
    }.unwrap();

    hashmap!["data".to_owned() => data]
}

pub fn component_add(_x: &yarrow::Add, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("add");
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x + y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x + y)]),
        _ => Err("Add: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}


pub fn component_subtract(_x: &yarrow::Subtract, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x - y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x - y)]),
        _ => Err("Subtract: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_divide(_x: &yarrow::Divide, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x / y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x / y)]),
        _ => Err("Divide: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_multiply(_x: &yarrow::Multiply, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (FieldEvaluation::F64(x), FieldEvaluation::F64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(x * y)]),
        (FieldEvaluation::I64(x), FieldEvaluation::I64(y)) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(x * y)]),
        _ => Err("Multiply: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_power(_x: &yarrow::Power, arguments: &NodeArguments) -> NodeEvaluation {
    let power: f64 = get_f64(&arguments, "right");
    let data = get_array_f64(&arguments, "left");
    hashmap!["data".to_string() => FieldEvaluation::F64(data.mapv(|x| x.powf(power)))]
}

pub fn component_negate(_x: &yarrow::Negate, arguments: &NodeArguments) -> NodeEvaluation {
    match arguments.get("data").unwrap() {
        FieldEvaluation::F64(x) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::F64(-x)]),
        FieldEvaluation::I64(x) =>
            Ok(hashmap!["data".to_string() => FieldEvaluation::I64(-x)]),
        _ => Err("Negate: Argument must be numeric.")
    }.unwrap()
}

pub fn component_impute_f64(_x: &yarrow::ImputeF64, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let min: f64 = get_f64(&arguments, "min");
    let max: f64 = get_f64(&arguments, "max");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::transformations::impute_f64(&data, &min, &max))]
}

pub fn component_impute_i64(_x: &yarrow::ImputeI64, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let min: i64 = get_i64(&arguments, "min");
    let max: i64 = get_i64(&arguments, "max");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::transformations::impute_i64(&data, &min, &max))]
}

pub fn component_bin(_X: &yarrow::Bin, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let edges: ArrayD<f64> = get_array_f64(&arguments, "edges");
    let inclusive_left: bool = get_bool(&arguments, "inclusive_left");
    hashmap!["data".to_string() => FieldEvaluation::Str(utilities::transformations::bin(&data, &edges, &inclusive_left))]
}

pub fn component_count(_X: &yarrow::Count, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("data").unwrap(), arguments.get("group_by").unwrap()) {
        (FieldEvaluation::F64(data), FieldEvaluation::F64(group_by)) => Ok(hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::count(&get_array_f64(&arguments, "data"), &Some(get_array_f64(&arguments, "group_by"))))]),
        (FieldEvaluation::Str(data), FieldEvaluation::Str(group_by)) => Ok(hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::count(&get_array_str(&arguments, "data"), &Some(get_array_str(&arguments, "group_by"))))]),
        (FieldEvaluation::Bool(data), FieldEvaluation::Bool(group_by)) => Ok(hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::count(&get_array_bool(&arguments, "data"), &Some(get_array_bool(&arguments, "group_by"))))]),
        _ => Err("Count: Data type must be f64, string, or bool")
    }.unwrap()
}

// pub fn component_histogram(_X: &yarrow::Bin, argument: &NodeArguments) -> NodeEvaluation {
//     let data: ArrayD<f64> = get_array_f64(&arguments, "data");
//     let edges: ArrayD<f64> = get_array_f64(&arguments, "edges");
//     let inclusive_left: bool = get_bool(&arguments, "inclusive_left");
//     hashmap!["data".to_string() => FieldEvaluation::String_F64_HashMap(utilities::aggregations::histogram(&data, &edges, &inclusive_left)]
// }

pub fn component_mean(_x: &yarrow::Mean, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::mean(&data))]
}

pub fn component_variance(_x: &yarrow::Variance, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let finite_sample_correction: bool = get_bool(&arguments, "finite_sample_correction");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::variance(&data, &finite_sample_correction))]
}

pub fn component_median(_x: &yarrow::Median, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::median(&data))]
}

pub fn component_sum(_x: &yarrow::Sum, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::aggregations::sum(&data))]
}

pub fn component_laplace_mechanism(_x: &yarrow::LaplaceMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let sensitivity: f64 = get_f64(&arguments, "sensitivity");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::mechanisms::laplace_mechanism(&data, &epsilon, &sensitivity))]
}

pub fn component_gaussian_mechanism(_x: &yarrow::GaussianMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let delta: f64 = get_f64(&arguments, "delta");
    let sensitivity: f64 = get_f64(&arguments, "sensitivity");
    hashmap!["data".to_string() => FieldEvaluation::F64(utilities::mechanisms::gaussian_mechanism(&data, &epsilon, &delta, &sensitivity))]
}

pub fn component_simple_geometric_mechanism(_x: &yarrow::SimpleGeometricMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let count_min: f64 = get_f64(&arguments, "count_min");
    let count_max: f64 = get_f64(&arguments, "count_max");
    let enforce_constant_time: bool = get_bool(&arguments, "enforce_constant_time");
    hashmap!["data".to_string() =>
        FieldEvaluation::F64(utilities::mechanisms::simple_geometric_mechanism(
                             &data, &epsilon, &count_min, &count_max, &enforce_constant_time))]
}

// TODO: Possibly compute sensitivity here, and pass into algorithm?

pub fn component_dp_mean(component: &yarrow::DpMean, arguments: &NodeArguments) -> NodeEvaluation {
    let data: FieldEvaluation = match yarrow::Mechanism::from_i32(component.mechanism).unwrap() {
        yarrow::Mechanism::Laplace => Ok(FieldEvaluation::F64(Array::from_elem((), algorithms::dp_mean_laplace(
            component.epsilon,
            get_f64(&arguments, "num_records"),
            get_array_f64(&arguments, "data"),
            get_f64(&arguments, "minimum"),
            get_f64(&arguments, "maximum")
        )).into_dyn())),
        _ => Err("Mean: Unknown algorithm type.")
    }.unwrap();
    // println!("dpmean");
    hashmap!["data".to_string() => data]
}

pub fn component_dp_variance(component: &yarrow::DpVariance, arguments: &NodeArguments) -> NodeEvaluation {
    let data: FieldEvaluation = match yarrow::Mechanism::from_i32(component.mechanism).unwrap() {
        yarrow::Mechanism::Laplace => Ok(FieldEvaluation::F64(Array::from_elem((), algorithms::dp_variance_laplace(
            component.epsilon,
            get_f64(&arguments, "num_records"),
            get_array_f64(&arguments, "data"),
            get_f64(&arguments, "minimum"),
            get_f64(&arguments, "maximum")
        )).into_dyn())),
        _ => Err("Variance: Unknown algorithm type.")
    }.unwrap();
    hashmap!["data".to_string() => data]
}

pub fn component_dp_moment_raw(component: &yarrow::DpMomentRaw, arguments: &NodeArguments) -> NodeEvaluation {
    let data: FieldEvaluation = match yarrow::Mechanism::from_i32(component.mechanism).unwrap() {
        yarrow::Mechanism::Laplace => Ok(FieldEvaluation::F64(Array::from_elem((), algorithms::dp_moment_raw_laplace(
            component.epsilon,
            get_f64(&arguments, "num_records"),
            get_array_f64(&arguments, "data"),
            get_f64(&arguments, "minimum"),
            get_f64(&arguments, "maximum"),
            component.order
        )).into_dyn())),
        _ => Err("Moment Raw: Unknown algorithm type.")
    }.unwrap();
    hashmap!["data".to_string() => data]
}


pub fn component_dp_covariance(component: &yarrow::DpCovariance, arguments: &NodeArguments) -> NodeEvaluation {
    let data: FieldEvaluation = match yarrow::Mechanism::from_i32(component.mechanism).unwrap() {
        yarrow::Mechanism::Laplace => Ok(FieldEvaluation::F64(Array::from_elem((), algorithms::dp_covariance(
            component.epsilon,
            get_f64(&arguments, "num_records"),
            get_array_f64(&arguments, "data_x"),
            get_array_f64(&arguments, "data_y"),
            get_f64(&arguments, "minimum_x"),
            get_f64(&arguments, "minimum_y"),
            get_f64(&arguments, "maximum_x"),
            get_f64(&arguments, "maximum_y")
        )).into_dyn())),
        _ => Err("Covariance: Unknown algorithm type.")
    }.unwrap();
    hashmap!["data".to_string() => data]
}
