extern crate yarrow_validator;
use yarrow_validator::yarrow;

use ndarray::prelude::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::iter::FromIterator;

use crate::components;

// equivalent to proto ArrayNd
#[derive(Debug)]
pub enum NodeEvaluation {
    Bytes(ArrayD<u8>), // bytes::Bytes BROKEN: only one byte is stored
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
    // String_F64_HashMap(HashMap<String, f64>)
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

        if is_privatizer(&component) {
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

pub fn is_privatizer(component: &yarrow::Component) -> bool {
    use yarrow::component::Value::*;
    match component.to_owned().value.unwrap() {
        Dpmean(_x) => true,
        _ => false
    }
}

pub fn execute_graph(analysis: &yarrow::Analysis,
                     release: &yarrow::Release,
                     dataset: &yarrow::Dataset) -> yarrow::Release {

    let node_ids_release: HashSet<u32> = get_release_nodes(&analysis);

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(get_sinks(&analysis).into_iter());

    let mut evaluations = release_to_evaluations(release);
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

            evaluations.insert(node_id, execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, &dataset));

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
                         dataset: &yarrow::Dataset) -> NodeEvaluation {

    let arguments = get_arguments(&component, &evaluations);

    match component.to_owned().value.unwrap() {
        yarrow::component::Value::Literal(x) => components::component_literal(&x),
        yarrow::component::Value::Datasource(x) => components::component_datasource(&x, &dataset, &arguments),
        yarrow::component::Value::Add(x) => components::component_add(&x, &arguments),
        yarrow::component::Value::Subtract(x) => components::component_subtract(&x, &arguments),
        yarrow::component::Value::Divide(x) => components::component_divide(&x, &arguments),
        yarrow::component::Value::Multiply(x) => components::component_multiply(&x, &arguments),
        yarrow::component::Value::Power(x) => components::component_power(&x, &arguments),
        yarrow::component::Value::Negate(x) => components::component_negate(&x, &arguments),
        yarrow::component::Value::ImputeF64Uniform(x) => components::component_impute_f64_uniform(&x, &arguments),
        yarrow::component::Value::ImputeF64Gaussian(x) => components::component_impute_f64_gaussian(&x, &arguments),
        yarrow::component::Value::ImputeI64Uniform(x) => components::component_impute_i64_uniform(&x, &arguments),
        yarrow::component::Value::Bin(x) => components::component_bin(&x, &arguments),
        // yarrow::component::Value::Count(x) => components::component_count(&x, &arguments),
        // yarrow::component::Value::Histogram(x) => components::component_histogram(&x, &arguments),
        yarrow::component::Value::Mean(x) => components::component_mean(&x, &arguments),
        yarrow::component::Value::Median(x) => components::component_median(&x, &arguments),
        yarrow::component::Value::Sum(x) => components::component_sum(&x, &arguments),
        yarrow::component::Value::Variance(x) => components::component_variance(&x, &arguments),
        yarrow::component::Value::KthRawSampleMoment(x) => components::component_kth_raw_sample_moment(&x, &arguments),
        yarrow::component::Value::LaplaceMechanism(x) => components::component_laplace_mechanism(&x, &arguments),
        yarrow::component::Value::GaussianMechanism(x) => components::component_gaussian_mechanism(&x, &arguments),
        yarrow::component::Value::SimpleGeometricMechanism(x) => components::component_simple_geometric_mechanism(&x, &arguments),
        // yarrow::component::Value::Dpmean(x) => components::component_dp_mean(&x, &arguments),
        // yarrow::component::Value::Dpvariance(x) => components::component_dp_variance(&x, &arguments),
        // yarrow::component::Value::Dpmomentraw(x) => components::component_dp_moment_raw(&x, &arguments),
        // yarrow::component::Value::Dpcovariance(x) => components::component_dp_covariance(&x, &arguments),
        // TODO: return an error result
        _ => NodeEvaluation::Bool(array![false].into_dyn())
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

pub fn release_to_evaluations(release: &yarrow::Release) -> GraphEvaluation {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        evaluations.insert(*node_id, parse_proto_array(node_release));
    }
    evaluations
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> yarrow::Release {
    let mut releases = HashMap::new();
    for (node_id, node_eval) in evaluations {
        releases.insert(*node_id, serialize_proto_array(&node_eval));
    }
    yarrow::Release {
        values: releases
    }
}

pub fn parse_proto_array(value: &yarrow::ArrayNd) -> NodeEvaluation {
    let value = value.to_owned();
    let shape: Vec<usize> = value.shape.iter().map(|x| *x as usize).collect();
    match value.data.unwrap() {
        yarrow::array_nd::Data::Bytes(x) =>
            NodeEvaluation::Bytes(Array::from_shape_vec(shape, x).unwrap().into_dyn()),
        yarrow::array_nd::Data::Bool(x) =>
            NodeEvaluation::Bool(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()),
        yarrow::array_nd::Data::I64(x) =>
            NodeEvaluation::I64(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()),
        yarrow::array_nd::Data::F64(x) =>
            NodeEvaluation::F64(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()),
        yarrow::array_nd::Data::String(x) =>
            NodeEvaluation::Str(Array::from_shape_vec(shape, x.data).unwrap().into_dyn()),
    }
}

pub fn serialize_proto_array(evaluation: &NodeEvaluation) -> yarrow::ArrayNd {

    match evaluation {
        NodeEvaluation::Bytes(x) => yarrow::ArrayNd {
            datatype: yarrow::DataType::Bytes as i32,
            data: Some(yarrow::array_nd::Data::Bytes(x.iter().map(|s| *s).collect())),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        NodeEvaluation::Bool(x) => yarrow::ArrayNd {
            datatype: yarrow::DataType::Bool as i32,
            data: Some(yarrow::array_nd::Data::Bool(yarrow::Array1Dbool {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        NodeEvaluation::I64(x) => yarrow::ArrayNd {
            datatype: yarrow::DataType::I64 as i32,
            data: Some(yarrow::array_nd::Data::I64(yarrow::Array1Di64 {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        NodeEvaluation::F64(x) => yarrow::ArrayNd {
            datatype: yarrow::DataType::F64 as i32,
            data: Some(yarrow::array_nd::Data::F64(yarrow::Array1Df64 {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        NodeEvaluation::Str(x) => yarrow::ArrayNd {
            datatype: yarrow::DataType::String as i32,
            data: Some(yarrow::array_nd::Data::String(yarrow::Array1Dstr {
                data: x.iter().cloned().collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
    }
}
