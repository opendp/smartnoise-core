extern crate yarrow_validator;
use yarrow_validator::proto;
use yarrow_validator::utilities::graph as yarrow_graph;

use ndarray::prelude::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::iter::FromIterator;

use crate::components;

// equivalent to proto Value
#[derive(Debug)]
pub enum NodeEvaluation {
//    Bytes(bytes::Bytes),
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
    HashmapString(HashMap<String, NodeEvaluation>)
}

// equivalent to proto Release
pub type GraphEvaluation = HashMap<u32, NodeEvaluation>;

// arguments to a node prior to evaluation
pub type NodeArguments<'a> = HashMap<String, &'a NodeEvaluation>;

pub fn get_arguments<'a>(component: &proto::Component, graph_evaluation: &'a GraphEvaluation) -> NodeArguments<'a> {
    let mut arguments = NodeArguments::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: &'a NodeEvaluation = graph_evaluation.get(&field).unwrap().to_owned();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}

pub fn execute_graph(analysis: &proto::Analysis,
                     release: &proto::Release,
                     dataset: &proto::Dataset) -> Result<proto::Release, String> {

    let node_ids_release: HashSet<u32> = yarrow_graph::get_release_nodes(&analysis)?;

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(yarrow_graph::get_sinks(&analysis).into_iter());

    let mut evaluations = release_to_evaluations(release)?;

    let graph: &HashMap<u32, proto::Component> = &analysis.graph;

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents.entry(*source_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();
        let component = graph.get(&node_id).unwrap();
        let arguments = component.to_owned().arguments;

        // discover if any dependencies remain uncomputed
        let mut evaluable = true;
        for source_node_id in arguments.values() {
            if !evaluations.contains_key(&source_node_id) {
                evaluable = false;
                traversal.push(*source_node_id);
                break;
            }
        }

        // check if all arguments are available
        if evaluable {
            traversal.pop();

            let evaluation = execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, &dataset)?;

            evaluations.insert(node_id, evaluation);

            // remove references to parent node, and if empty and private
            for argument_node_id in arguments.values() {
                let tempval = parents.get_mut(argument_node_id).unwrap();
                tempval.remove(&node_id);
                if parents.get(argument_node_id).unwrap().len() == 0 {
                    if !node_ids_release.contains(argument_node_id) {
                        evaluations.remove(argument_node_id);
                        // parents.remove(argument_node_id); // optional
                    }
                }
            }
        }
    }
    evaluations_to_release(&evaluations)
}

pub fn execute_component(component: &proto::Component,
                         evaluations: &GraphEvaluation,
                         dataset: &proto::Dataset) -> Result<NodeEvaluation, String> {

    let arguments = get_arguments(&component, &evaluations);

    use proto::component::Value as Value;
    match component.to_owned().value.unwrap() {
        Value::Literal(x) => components::component_literal(&x),
        Value::Datasource(x) => components::component_datasource(&x, &dataset, &arguments),
        Value::Add(x) => components::component_add(&x, &arguments),
        Value::Subtract(x) => components::component_subtract(&x, &arguments),
        Value::Divide(x) => components::component_divide(&x, &arguments),
        Value::Multiply(x) => components::component_multiply(&x, &arguments),
        Value::Power(x) => components::component_power(&x, &arguments),
        Value::Negate(x) => components::component_negate(&x, &arguments),
        Value::ImputeFloatUniform(x) => components::component_impute_float_uniform(&x, &arguments),
        Value::ImputeFloatGaussian(x) => components::component_impute_float_gaussian(&x, &arguments),
        Value::ImputeIntUniform(x) => components::component_impute_int_uniform(&x, &arguments),
        Value::Bin(x) => components::component_bin(&x, &arguments),
        Value::Rowmin(x) => components::component_row_wise_min(&x, &arguments),
        Value::Rowmax(x) => components::component_row_wise_max(&x, &arguments),
        // Value::Count(x) => components::component_count(&x, &arguments),
        // Value::Histogram(x) => components::component_histogram(&x, &arguments),
        Value::Mean(x) => components::component_mean(&x, &arguments),
        Value::Median(x) => components::component_median(&x, &arguments),
        Value::Sum(x) => components::component_sum(&x, &arguments),
        Value::Variance(x) => components::component_variance(&x, &arguments),
//        Value::Kthsamplemoment(x) => components::component_kth_sample_moment(&x, &arguments),
        Value::LaplaceMechanism(x) => components::component_laplace_mechanism(&x, &arguments),
        Value::GaussianMechanism(x) => components::component_gaussian_mechanism(&x, &arguments),
        Value::SimpleGeometricMechanism(x) => components::component_simple_geometric_mechanism(&x, &arguments),
        variant => Err(format!("Component type not implemented: {:?}", variant))
    }
}

pub fn get_f64(arguments: &NodeArguments, column: &str) -> f64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        NodeEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        NodeEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be numeric")
    }.unwrap()
}

pub fn get_array_f64(arguments: &NodeArguments, column: &str) -> ArrayD<f64> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.mapv(|v| if v {1.} else {0.})),
        NodeEvaluation::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
        NodeEvaluation::F64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be numeric")
    }.unwrap()
}

pub fn get_i64(arguments: &NodeArguments, column: &str) -> i64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1} else {0}),
        NodeEvaluation::I64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be integer")
    }.unwrap()
}

pub fn get_array_i64(arguments: &NodeArguments, column: &str) -> ArrayD<i64> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.mapv(|v| if v {1} else {0})),
        NodeEvaluation::I64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() +" must be integer")
    }.unwrap()
}

pub fn get_str(arguments: &NodeArguments, column: &str) -> String {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Str(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() +" must be string")
    }.unwrap()
}

pub fn get_array_str(arguments: &NodeArguments, column: &str) -> ArrayD<String> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Str(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be string")
    }.unwrap()
}

pub fn get_bool(arguments: &NodeArguments, column: &str) -> bool {
    match arguments.get(column).unwrap() {
        // maybe want to figure out how to accept wider range of bool arguments -- for now, comment out
        // Mike: @ above ^^ : the Equal component is sufficient for more complex casting. It is safe to assume bool data here
        // (NodeEvaluation::F64(x) && (*x.first().unwrap() == 1. || x.first().unwrap() == 0.)) => Ok(if *x.first().unwrap() == 1. {true} else *x.first().unwrap() == 0. {false}),
        // (NodeEvaluation::I64(x) && (*x.first().unwrap() == 1 || x.first().unwrap() == 0)) => Ok(if *x.first().unwrap() == 1 {true} else *x.first().unwrap() == 0 {false}),
        // (NodeEvaluation::Str(x) && (*x.first().unwrap() == "true" || x.first().unwrap() == "false")) => Ok(x.first().parse::<bool>().unwrap().to_owned()),
        NodeEvaluation::Bool(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() +" must be boolean")
    }.unwrap()
}

pub fn get_array_bool(arguments: &NodeArguments, column: &str) -> ArrayD<bool> {
    match arguments.get(column).unwrap() {
        // maybe want to figure out how to accept wider range of bool arguments -- for now, comment out
        // Mike: @ above ^^ : the Equal component is sufficient for more complex casting. It is safe to assume bool data here
        // (NodeEvaluation::F64(x) && (x.mapv(|v| vec![0., 1.].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == 1. {true} else if {false})),
        // (NodeEvaluation::I64(x) && (*x.mapv(|v| vec![0, 1].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == 1 {true} else if v == 0 {false})),
        // (NodeEvaluation::Str(x) && (*x.mapv(|v| vec!["false","true"].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == "true" {true} else if v == "false" {false})),
        NodeEvaluation::Bool(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be boolean")
    }.unwrap()
}

pub fn release_to_evaluations(release: &proto::Release) -> Result<GraphEvaluation, String> {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        evaluations.insert(*node_id, parse_proto_value(&node_release.value.to_owned().unwrap())?);
    }
    Ok(evaluations)
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> Result<proto::Release, String> {
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();
    for (node_id, node_eval) in evaluations {
        if let Ok(array_serialized) = serialize_proto_value(node_eval) {
            releases.insert(*node_id, proto::ReleaseNode{
                value: Some(array_serialized), privacy_usage: None
            });
        }
    }
    Ok(proto::Release {
        values: releases
    })
}

pub fn parse_proto_value(value: &proto::Value) -> Result<NodeEvaluation, String> {
    let value = value.to_owned().data;
    if value.is_none() {
        return Err("proto value is empty".to_string())
    }
    match value.unwrap() {
        proto::value::Data::ArrayNd(arrayND) => match arrayND.flattened {
            Some(flattened) => match flattened.data {
                Some(data) => match data {
                    proto::array1_d::Data::Bool(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::Bool(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    },
                    proto::array1_d::Data::I64(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::I64(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    },
                    proto::array1_d::Data::F64(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::F64(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    },
                    proto::array1_d::Data::String(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::Str(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    },
                    _ => return Err("unsupported proto array variant encountered".to_string())
                },
                None => return Err("proto array is empty".to_string())
            },
            None => return Err("proto array is empty".to_string())
        },
        proto::value::Data::HashmapString(hash_map) => {
            let mut evaluation: HashMap<String, NodeEvaluation> = HashMap::new();
            for (node_id, value) in &hash_map.data {
                let parsed_result = parse_proto_value(value);
                if let Ok(parsed) = parsed_result {
                    evaluation.insert(node_id.to_owned(), parsed);
                } else {
                    return parsed_result;
                }
            }
            Ok(NodeEvaluation::HashmapString(evaluation))
        }
//        proto::array_nd::Data::Bytes(x) =>
//            NodeEvaluation::Bytes(bytes::Bytes::from(x)),
        _ => Err("unsupported proto value variant encountered".to_string())
    }
}

pub fn serialize_proto_value(evaluation: &NodeEvaluation) -> Result<proto::Value, String> {

    match evaluation {
//        NodeEvaluation::Bytes(x) => proto::Value {
//            datatype: proto::DataType::Bytes as i32,
//            data: Some(proto::value::Data::Bytes(prost::encoding::bytes::encode(x)))
//        },
        NodeEvaluation::Bool(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::Bool(proto::Array1Dbool {
                        data: x.iter().map(|s| *s).collect(),
                    }))
                }),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::I64(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::I64(proto::Array1Di64 {
                        data: x.iter().map(|s| *s).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::F64(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::F64(proto::Array1Df64 {
                        data: x.iter().map(|s| *s).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::Str(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::String(proto::Array1Dstr {
                        data: x.iter().map(|s| s.to_owned()).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::HashmapString(x) => {
            let mut evaluation_serialized: HashMap<String, proto::Value> = HashMap::new();
            for (node_id, node_eval) in x {
                if let Ok(array_serialized) = serialize_proto_value(node_eval) {
                    evaluation_serialized.insert(node_id.to_owned(), array_serialized);
                }
            }

            return Ok(proto::Value {
                data: Some(proto::value::Data::HashmapString(proto::HashmapString {
                    data: evaluation_serialized
                }))
            })
        },
        _ => Err("Unsupported evaluation type. Could not serialize data to protobuf.".to_string())
    }
}
