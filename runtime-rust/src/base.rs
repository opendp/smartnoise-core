extern crate yarrow_validator;
use yarrow_validator::yarrow;

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

pub fn get_arguments<'a>(component: &yarrow::Component, graph_evaluation: &'a GraphEvaluation) -> NodeArguments<'a> {
    let mut arguments = NodeArguments::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: &'a NodeEvaluation = graph_evaluation.get(&field).unwrap().to_owned();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}

pub fn get_release_nodes(analysis: &yarrow::Analysis) -> HashSet<u32> {

    let mut release_node_ids = HashSet::<u32>::new();
    // assume sinks are private
    let sink_node_ids = get_sinks(analysis);
//    println!("sink nodes: {:?}", sink_node_ids);

    // traverse back through arguments until privatizers found
    let mut node_queue = VecDeque::from_iter(sink_node_ids.iter());

    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    while !node_queue.is_empty() {
        let node_id = node_queue.pop_front().unwrap();
        let component = graph.get(&node_id).unwrap();

        if !component.omit {
            release_node_ids.insert(*node_id);
        }
        else {
            for source_node_id in component.arguments.values() {
                node_queue.push_back(&source_node_id);
            }
        }
    }

    return release_node_ids;
}

pub fn get_sinks(analysis: &yarrow::Analysis) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    // start with all nodes
    for node_id in analysis.graph.keys() {
        node_ids.insert(*node_id);
    }

    // remove nodes that are referenced in arguments
    for node in analysis.graph.values() {
        for source_node_id in node.arguments.values() {
            node_ids.remove(&source_node_id);
        }
    }

    // move to heap, transfer ownership to caller
    return node_ids.to_owned();
}

pub fn execute_graph(analysis: &yarrow::Analysis,
                     release: &yarrow::Release,
                     dataset: &yarrow::Dataset) -> Result<yarrow::Release, &'static str> {

    let node_ids_release: HashSet<u32> = get_release_nodes(&analysis);

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(get_sinks(&analysis).into_iter());

    let evaluations_result = release_to_evaluations(release);
    if let Err(message) = evaluations_result {
        return Err(message)
    }
    let mut evaluations = evaluations_result.unwrap();
    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

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
                &graph.get(&node_id).unwrap(), &evaluations, &dataset);

            if let Err(message) = evaluation {
                return Err(message);
            }

            evaluations.insert(node_id, evaluation.unwrap());

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

pub fn execute_component(component: &yarrow::Component,
                         evaluations: &GraphEvaluation,
                         dataset: &yarrow::Dataset) -> Result<NodeEvaluation, &'static str> {

    let arguments = get_arguments(&component, &evaluations);

    match component.to_owned().value.unwrap() {
        yarrow::component::Value::Literal(x) => Ok(components::component_literal(&x)),
        yarrow::component::Value::Datasource(x) => Ok(components::component_datasource(&x, &dataset, &arguments)),
        yarrow::component::Value::Add(x) => Ok(components::component_add(&x, &arguments)),
        yarrow::component::Value::Subtract(x) => Ok(components::component_subtract(&x, &arguments)),
        yarrow::component::Value::Divide(x) => Ok(components::component_divide(&x, &arguments)),
        yarrow::component::Value::Multiply(x) => Ok(components::component_multiply(&x, &arguments)),
        yarrow::component::Value::Power(x) => Ok(components::component_power(&x, &arguments)),
        yarrow::component::Value::Negate(x) => Ok(components::component_negate(&x, &arguments)),
        yarrow::component::Value::ImputeF64Uniform(x) => Ok(components::component_impute_f64_uniform(&x, &arguments)),
        yarrow::component::Value::ImputeF64Gaussian(x) => Ok(components::component_impute_f64_gaussian(&x, &arguments)),
        yarrow::component::Value::ImputeI64Uniform(x) => Ok(components::component_impute_i64_uniform(&x, &arguments)),
        yarrow::component::Value::Bin(x) => Ok(components::component_bin(&x, &arguments)),
        // yarrow::component::Value::Count(x) => Ok(components::component_count(&x, &arguments)),
        // yarrow::component::Value::Histogram(x) => Ok(components::component_histogram(&x, &arguments)),
        yarrow::component::Value::Mean(x) => Ok(components::component_mean(&x, &arguments)),
        yarrow::component::Value::Median(x) => Ok(components::component_median(&x, &arguments)),
        yarrow::component::Value::Sum(x) => Ok(components::component_sum(&x, &arguments)),
        yarrow::component::Value::Variance(x) => Ok(components::component_variance(&x, &arguments)),
//        yarrow::component::Value::Kthsamplemoment(x) => Ok(components::component_kth_sample_moment(&x, &arguments)),
        yarrow::component::Value::LaplaceMechanism(x) => Ok(components::component_laplace_mechanism(&x, &arguments)),
        yarrow::component::Value::GaussianMechanism(x) => Ok(components::component_gaussian_mechanism(&x, &arguments)),
        yarrow::component::Value::SimpleGeometricMechanism(x) => Ok(components::component_simple_geometric_mechanism(&x, &arguments)),
        _ => Err("Component type not implemented.")
    }
}

pub fn get_f64(arguments: &NodeArguments, column: &str) -> f64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        NodeEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        NodeEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() +" must be numeric")
    }.unwrap()
}

pub fn get_array_f64(arguments: &NodeArguments, column: &str) -> ArrayD<f64> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.mapv(|v| if v {1.} else {0.})),
        NodeEvaluation::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
        NodeEvaluation::F64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() +" must be numeric")
    }.unwrap()
}

pub fn get_i64(arguments: &NodeArguments, column: &str) -> i64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1} else {0}),
        NodeEvaluation::I64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() +" must be integer")
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
        // (NodeEvaluation::F64(x) && (x.mapv(|v| vec![0., 1.].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == 1. {true} else if {false})),
        // (NodeEvaluation::I64(x) && (*x.mapv(|v| vec![0, 1].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == 1 {true} else if v == 0 {false})),
        // (NodeEvaluation::Str(x) && (*x.mapv(|v| vec!["false","true"].contains(v)).all(|v| v == true))) => Ok(x.mapv(|v| if v == "true" {true} else if v == "false" {false})),
        NodeEvaluation::Bool(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be boolean")
    }.unwrap()
}

pub fn release_to_evaluations(release: &yarrow::Release) -> Result<GraphEvaluation, &'static str> {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        let parsed_result = parse_proto_array(node_release);
        if let Err(message) = parsed_result {
            return Err(message);
        }
        let parsed = parsed_result.unwrap();
        evaluations.insert(*node_id, parsed);

    }
    Ok(evaluations)
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> Result<yarrow::Release, &'static str> {
    let mut releases: HashMap<u32, yarrow::Value> = HashMap::new();
    for (node_id, node_eval) in evaluations {
        if let Ok(array_serialized) = serialize_proto_array(node_eval) {
            releases.insert(*node_id, array_serialized);
        }
    }
    Ok(yarrow::Release {
        values: releases
    })
}

pub fn parse_proto_array(value: &yarrow::Value) -> Result<NodeEvaluation, &'static str> {
    let value = value.to_owned();
    match value.data.unwrap() {
//        yarrow::array_nd::Data::Bytes(x) =>
//            NodeEvaluation::Bytes(bytes::Bytes::from(x)),
        yarrow::value::Data::Bool(x) => {
            let shape: Vec<usize> = x.shape.iter().map(|x| *x as usize).collect();
            Ok(NodeEvaluation::Bool(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()))
        },
        yarrow::value::Data::I64(x) => {
            let shape: Vec<usize> = x.shape.iter().map(|x| *x as usize).collect();
            Ok(NodeEvaluation::I64(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()))
        },
        yarrow::value::Data::F64(x) => {
            let shape: Vec<usize> = x.shape.iter().map(|x| *x as usize).collect();
            Ok(NodeEvaluation::F64(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()))
        },
        yarrow::value::Data::String(x) => {
            let shape: Vec<usize> = x.shape.iter().map(|x| *x as usize).collect();
            Ok(NodeEvaluation::Str(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()))
        },
        yarrow::value::Data::HashmapString(x) => {
            let mut evaluation: HashMap<String, NodeEvaluation> = HashMap::new();
            for (node_id, value) in &x.data {
                let parsed_result = parse_proto_array(value);
                if let Ok(parsed) = parsed_result {
                    evaluation.insert(node_id.to_owned(), parsed);
                } else {
                    return parsed_result;
                }
            }
            Ok(NodeEvaluation::HashmapString(evaluation))
        }
        _ => Err("Unsupported proto value encountered.")
    }
}

pub fn serialize_proto_array(evaluation: &NodeEvaluation) -> Result<yarrow::Value, &'static str> {

    match evaluation {
//        NodeEvaluation::Bytes(x) => yarrow::Value {
//            datatype: yarrow::DataType::Bytes as i32,
//            data: Some(yarrow::value::Data::Bytes(prost::encoding::bytes::encode(x)))
//        },
        NodeEvaluation::Bool(x) => Ok(yarrow::Value {
            datatype: yarrow::DataType::Bool as i32,
            data: Some(yarrow::value::Data::Bool(yarrow::Array1Dbool {
                data: x.iter().map(|s| *s).collect(),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::I64(x) => Ok(yarrow::Value {
            datatype: yarrow::DataType::I64 as i32,
            data: Some(yarrow::value::Data::I64(yarrow::Array1Di64 {
                data: x.iter().map(|s| *s).collect(),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::F64(x) => Ok(yarrow::Value {
            datatype: yarrow::DataType::F64 as i32,
            data: Some(yarrow::value::Data::F64(yarrow::Array1Df64 {
                data: x.iter().map(|s| *s).collect(),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::Str(x) => Ok(yarrow::Value {
            datatype: yarrow::DataType::String as i32,
            data: Some(yarrow::value::Data::String(yarrow::Array1Dstr {
                data: x.iter().cloned().collect(),
                order: (1..x.ndim()).map(|x| {x as u64}).collect(),
                shape: x.shape().iter().map(|y| {*y as u64}).collect()
            }))
        }),
        NodeEvaluation::HashmapString(x) => {
            let mut evaluation_serialized: HashMap<String, yarrow::Value> = HashMap::new();
            for (node_id, node_eval) in x {
                if let Ok(array_serialized) = serialize_proto_array(node_eval) {
                    evaluation_serialized.insert(node_id.to_owned(), array_serialized);
                }
            }

            return Ok(yarrow::Value {
                datatype: yarrow::DataType::HashmapString as i32,
                data: Some(yarrow::value::Data::HashmapString(yarrow::HashmapString {
                    data: evaluation_serialized
                }))
            })
        },
        _ => Err("Unsupported evaluation type. Could not serialize data to protobuf.")
    }
}
