extern crate yarrow_validator;
use yarrow_validator::yarrow;

use ndarray::prelude::*;
use crate::base::*;
use std::collections::HashMap;
extern crate csv;
use std::str::FromStr;
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
    parse_proto_array(&x.to_owned().value.unwrap()).unwrap()
}

//pub fn component_table(table: &yarrow::Table, dataset: &yarrow::Dataset, arguments: &NodeArguments) -> NodeEvaluation {
//    let table = dataset.tables.get(&datasource.dataset_id).unwrap();
//    match table.value.as_ref().unwrap() {
//        yarrow::table::Value::FilePath(path) => {
//        },
//
//    }
//}

pub fn component_datasource(datasource: &yarrow::DataSource, dataset: &yarrow::Dataset, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("datasource");

    let table = dataset.tables.get(&datasource.dataset_id).unwrap();
    match table.value.as_ref().unwrap() {
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
                NodeEvaluation::Str(x) => Ok(match x.first().unwrap().as_ref() {
//                    "BYTES" =>
//                        Ok(NodeEvaluation::Bytes(Array1::from(get_column::<u8>(&path, &datasource.column_id)).into_dyn())),
                    "BOOL" =>
                        Ok(NodeEvaluation::Bool(Array1::from(get_column::<bool>(&path, &datasource.column_id)).into_dyn())),
                    "I64" =>
                        Ok(NodeEvaluation::I64(Array1::from(get_column::<i64>(&path, &datasource.column_id)).into_dyn())),
                    "F64" =>
                        Ok(NodeEvaluation::F64(Array1::from(get_column::<f64>(&path, &datasource.column_id)).into_dyn())),
                    "STRING" =>
                        Ok(NodeEvaluation::Str(Array1::from(get_column::<String>(&path, &datasource.column_id)).into_dyn())),
                    _ => Err("Datatype is not recognized.")
                }.unwrap()),
                _ => Err("Datatype must be a string.")
            }
        },
        yarrow::table::Value::Literal(value) => parse_proto_array(&value),
        _ => Err("Only file paths are supported")
    }.unwrap()
}

pub fn component_add(_x: &yarrow::Add, arguments: &NodeArguments) -> NodeEvaluation {
//    println!("add");
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (NodeEvaluation::F64(x), NodeEvaluation::F64(y)) =>
            Ok(NodeEvaluation::F64(x + y)),
        (NodeEvaluation::I64(x), NodeEvaluation::I64(y)) =>
            Ok(NodeEvaluation::I64(x + y)),
        _ => Err("Add: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}


pub fn component_subtract(_x: &yarrow::Subtract, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (NodeEvaluation::F64(x), NodeEvaluation::F64(y)) =>
            Ok(NodeEvaluation::F64(x - y)),
        (NodeEvaluation::I64(x), NodeEvaluation::I64(y)) =>
            Ok(NodeEvaluation::I64(x - y)),
        _ => Err("Subtract: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_divide(_x: &yarrow::Divide, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (NodeEvaluation::F64(x), NodeEvaluation::F64(y)) =>
            Ok(NodeEvaluation::F64(x / y)),
        (NodeEvaluation::I64(x), NodeEvaluation::I64(y)) =>
            Ok(NodeEvaluation::I64(x / y)),
        _ => Err("Divide: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_multiply(_x: &yarrow::Multiply, arguments: &NodeArguments) -> NodeEvaluation {
    match (arguments.get("left").unwrap(), arguments.get("right").unwrap()) {
        (NodeEvaluation::F64(x), NodeEvaluation::F64(y)) =>
            Ok(NodeEvaluation::F64(x * y)),
        (NodeEvaluation::I64(x), NodeEvaluation::I64(y)) =>
            Ok(NodeEvaluation::I64(x * y)),
        _ => Err("Multiply: Either the argument types are mismatched or non-numeric.")
    }.unwrap()
}

pub fn component_power(_x: &yarrow::Power, arguments: &NodeArguments) -> NodeEvaluation {
    let power: f64 = get_f64(&arguments, "right");
    let data = get_array_f64(&arguments, "left");
    NodeEvaluation::F64(data.mapv(|x| x.powf(power)))
}

pub fn component_negate(_x: &yarrow::Negate, arguments: &NodeArguments) -> NodeEvaluation {
    match arguments.get("data").unwrap() {
        NodeEvaluation::F64(x) =>
            Ok(NodeEvaluation::F64(-x)),
        NodeEvaluation::I64(x) =>
            Ok(NodeEvaluation::I64(-x)),
        _ => Err("Negate: Argument must be numeric.")
    }.unwrap()
}

pub fn component_impute_f64_uniform(_x: &yarrow::ImputeF64Uniform, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let min: f64 = get_f64(&arguments, "min");
    let max: f64 = get_f64(&arguments, "max");
    NodeEvaluation::F64(utilities::transformations::impute_f64_uniform(&data, &min, &max))
}

pub fn component_impute_f64_gaussian(_x: &yarrow::ImputeF64Gaussian, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let shift: f64 = get_f64(&arguments, "shift");
    let scale: f64 = get_f64(&arguments, "scale");
    let min: f64 = get_f64(&arguments, "min");
    let max: f64 = get_f64(&arguments, "max");
    NodeEvaluation::F64(utilities::transformations::impute_f64_gaussian(&data, &shift, &scale, &min, &max))
}

pub fn component_impute_i64_uniform(_x: &yarrow::ImputeI64Uniform, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let min: i64 = get_i64(&arguments, "min");
    let max: i64 = get_i64(&arguments, "max");
    NodeEvaluation::F64(utilities::transformations::impute_i64_uniform(&data, &min, &max))
}

pub fn component_bin(_X: &yarrow::Bin, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let edges: ArrayD<f64> = get_array_f64(&arguments, "edges");
    let inclusive_left: bool = get_bool(&arguments, "inclusive_left");
    NodeEvaluation::Str(utilities::transformations::bin(&data, &edges, &inclusive_left))
}

pub fn component_clamp(_X: &yarrow::Clamp, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let min: f64 = get_f64(&arguments, "min");
    let max: f64 = get_f64(&arguments, "max");
    NodeEvaluation::F64(utilities::transformations::clamp(&data, &min, &max))
}

// pub fn component_count(_X: &yarrow::Count, arguments: &NodeArguments) -> NodeEvaluation {
//     match (arguments.get("data").unwrap(), arguments.get("group_by").unwrap()) {
//         (NodeEvaluation::F64(data), NodeEvaluation::F64(group_by)) =>
//             Ok(NodeEvaluation::F64(utilities::aggregations::count(&get_array_f64(&arguments, "data"), &Some(get_array_f64(&arguments, "group_by"))))),
//         (NodeEvaluation::Str(data), NodeEvaluation::Str(group_by)) =>
//             Ok(NodeEvaluation::F64(utilities::aggregations::count(&get_array_str(&arguments, "data"), &Some(get_array_str(&arguments, "group_by"))))),
//         (NodeEvaluation::Bool(data), NodeEvaluation::Bool(group_by)) =>
//             Ok(NodeEvaluation::F64(utilities::aggregations::count(&get_array_bool(&arguments, "data"), &Some(get_array_bool(&arguments, "group_by"))))),
//         _ => Err("Count: Data type must be f64, string, or bool")
//     }.unwrap()
// }

// pub fn component_histogram(_X: &yarrow::Bin, argument: &NodeArguments) -> NodeEvaluation {
//     let data: ArrayD<f64> = get_array_f64(&arguments, "data");
//     let edges: ArrayD<f64> = get_array_f64(&arguments, "edges");
//     let inclusive_left: bool = get_bool(&arguments, "inclusive_left");
//     NodeEvaluation::String_F64_HashMap(utilities::aggregations::histogram(&data, &edges, &inclusive_left)
// }

pub fn component_mean(_x: &yarrow::Mean, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    NodeEvaluation::F64(utilities::aggregations::mean(&data))
}

pub fn component_variance(_x: &yarrow::Variance, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let finite_sample_correction: bool = get_bool(&arguments, "finite_sample_correction");
    NodeEvaluation::F64(utilities::aggregations::variance(&data, &finite_sample_correction))
}

pub fn component_kth_raw_sample_moment(_x: &yarrow::KthRawSampleMoment, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    let k: i64 = get_i64(&arguments, "k");
    NodeEvaluation::F64(utilities::aggregations::kth_raw_sample_moment(&data, &k))
}

pub fn component_median(_x: &yarrow::Median, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    NodeEvaluation::F64(utilities::aggregations::median(&data))
}

pub fn component_sum(_x: &yarrow::Sum, arguments: &NodeArguments) -> NodeEvaluation {
    let data: ArrayD<f64> = get_array_f64(&arguments, "data");
    NodeEvaluation::F64(utilities::aggregations::sum(&data))
}

pub fn component_laplace_mechanism(_x: &yarrow::LaplaceMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let sensitivity: f64 = get_f64(&arguments, "sensitivity");
    NodeEvaluation::F64(utilities::mechanisms::laplace_mechanism(&epsilon, &sensitivity))
}

pub fn component_gaussian_mechanism(_x: &yarrow::GaussianMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let delta: f64 = get_f64(&arguments, "delta");
    let sensitivity: f64 = get_f64(&arguments, "sensitivity");
    NodeEvaluation::F64(utilities::mechanisms::gaussian_mechanism(&epsilon, &delta, &sensitivity))
}

pub fn component_simple_geometric_mechanism(_x: &yarrow::SimpleGeometricMechanism, arguments: &NodeArguments) -> NodeEvaluation {
    let epsilon: f64 = get_f64(&arguments, "epsilon");
    let sensitivity: f64 = get_f64(&arguments, "sensitivity");
    let count_min: i64 = get_i64(&arguments, "count_min");
    let count_max: i64 = get_i64(&arguments, "count_max");
    let enforce_constant_time: bool = get_bool(&arguments, "enforce_constant_time");
    NodeEvaluation::I64(utilities::mechanisms::simple_geometric_mechanism(
                             &epsilon, &sensitivity, &count_min, &count_max, &enforce_constant_time))
}
