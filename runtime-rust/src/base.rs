use ndarray::prelude::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;

use std::iter::FromIterator;

use crate::components;

// Include the `items` module, which is generated from items.proto.
pub mod burdock {
    include!(concat!(env!("OUT_DIR"), "/burdock.rs"));
}

// equivalent to proto ArrayNd
#[derive(Debug)]
pub enum FieldEvaluation {
    Bytes(ArrayD<u8>), // bytes::Bytes BROKEN: only one byte is stored
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
}

// equivalent to proto ReleaseNode
pub type NodeEvaluation = HashMap<String, FieldEvaluation>;
// equivalent to proto Release
pub type GraphEvaluation = HashMap<u32, NodeEvaluation>;

// arguments to a node prior to evaluation
pub type NodeArguments<'a> = HashMap<String, &'a FieldEvaluation>;

pub fn get_arguments<'a>(component: &burdock::Component, graph_evaluation: &'a GraphEvaluation) -> NodeArguments<'a> {
    let mut arguments = NodeArguments::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: &'a FieldEvaluation = graph_evaluation.get(&field.source_node_id).unwrap().get(&field.source_field).unwrap().to_owned();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}

pub fn get_release_nodes(analysis: &burdock::Analysis) -> HashSet<u32> {

    let mut release_node_ids = HashSet::<u32>::new();
    // assume sinks are private
    let sink_node_ids = get_sinks(analysis);
//    println!("sink nodes: {:?}", sink_node_ids);

    // traverse back through arguments until privatizers found
    let mut node_queue = VecDeque::from_iter(sink_node_ids.iter());

    let graph: &HashMap<u32, burdock::Component> = &analysis.graph;

    while !node_queue.is_empty() {
        let node_id = node_queue.pop_front().unwrap();
        let component = graph.get(&node_id).unwrap();

        if is_privatizer(&component) {
            release_node_ids.insert(*node_id);
        }
        else {
            for field in component.arguments.values() {
                node_queue.push_back(&field.source_node_id);
            }
        }
    }

    return release_node_ids;
}

pub fn get_sinks(analysis: &burdock::Analysis) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    // start with all nodes
    for node_id in analysis.graph.keys() {
        node_ids.insert(*node_id);
    }

    // remove nodes that are referenced in arguments
    for node in analysis.graph.values() {
        for field in node.arguments.values() {
            node_ids.remove(&field.source_node_id);
        }
    }

    // move to heap, transfer ownership to caller
    return node_ids.to_owned();
}

pub fn is_privatizer(component: &burdock::Component) -> bool {
    use burdock::component::Value::*;
    match component.to_owned().value.unwrap() {
        Dpmeanlaplace(_x) => true,
        _ => false
    }
}

pub fn execute_graph(analysis: &burdock::Analysis,
                     release: &burdock::Release,
                     dataset: &burdock::Dataset) -> burdock::Release {

    let node_ids_release: HashSet<u32> = get_release_nodes(&analysis);

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(get_sinks(&analysis).into_iter());

    let mut evaluations = release_to_evaluations(release);
    let graph: &HashMap<u32, burdock::Component> = &analysis.graph;

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|field| {
            let argument_node_id = &field.source_node_id;
            parents.entry(*argument_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();
        let component = graph.get(&node_id).unwrap();
        let arguments = component.to_owned().arguments;

        // discover if any dependencies remain uncomputed
        let mut evaluable = true;
        for field in arguments.values() {
            if !evaluations.contains_key(&field.source_node_id) {
                evaluable = false;
                traversal.push(field.source_node_id);
                break;
            }
        }

        // check if all arguments are available
        if evaluable {
            traversal.pop();

            evaluations.insert(node_id, execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, &dataset));

            // remove references to parent node, and if empty and private
            for argument in arguments.values() {
                let argument_node_id = &(argument.source_node_id);
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

pub fn execute_component(component: &burdock::Component,
                         evaluations: &GraphEvaluation,
                         dataset: &burdock::Dataset) -> NodeEvaluation {

    let arguments = get_arguments(&component, &evaluations);

    match component.to_owned().value.unwrap() {
        burdock::component::Value::Literal(x) => components::component_literal(&x),
        burdock::component::Value::Datasource(x) => components::component_datasource(&x, &dataset, &arguments),
        burdock::component::Value::Add(x) => components::component_add(&x, &arguments),
        burdock::component::Value::Subtract(x) => components::component_subtract(&x, &arguments),
        burdock::component::Value::Divide(x) => components::component_divide(&x, &arguments),
        burdock::component::Value::Multiply(x) => components::component_multiply(&x, &arguments),
        burdock::component::Value::Power(x) => components::component_power(&x, &arguments),
        burdock::component::Value::Negate(x) => components::component_negate(&x, &arguments),
        burdock::component::Value::Dpmeanlaplace(x) => components::component_dp_mean_laplace(&x, &arguments),
        _ => NodeEvaluation::new()
    }
}

pub fn get_f64(arguments: &NodeArguments, column: &str) -> f64 {
    match arguments.get(column).unwrap() {
        FieldEvaluation::Bool(x) => Ok(if *x.first().unwrap() {1.} else {0.}),
        FieldEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        FieldEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() +" must be numeric")
    }.unwrap()
}

pub fn get_array_f64(arguments: &NodeArguments, column: &str) -> ArrayD<f64> {
    match arguments.get(column).unwrap() {
        FieldEvaluation::Bool(x) => Ok(x.mapv(|v| if v {1.} else {0.})),
        FieldEvaluation::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
        FieldEvaluation::F64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() +" must be numeric")
    }.unwrap()
}

pub fn release_to_evaluations(release: &burdock::Release) -> GraphEvaluation {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        let mut evaluations_node = NodeEvaluation::new();
        for (field_id, field_release) in &node_release.values {
            evaluations_node.insert(field_id.to_owned(), parse_proto_array(&field_release));
        }
        evaluations.insert(*node_id, evaluations_node);
    }
    evaluations
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> burdock::Release {
    let mut releases = HashMap::new();
    for (node_id, node_eval) in evaluations {
        let mut node_release = HashMap::new();

        for (field_name, field_eval) in node_eval {
            node_release.insert(field_name.to_owned(), serialize_proto_array(&field_eval));
        }
        releases.insert(*node_id, burdock::ReleaseNode {
            values: node_release.to_owned()
        });
    }
    burdock::Release {
        values: releases
    }
}

pub fn parse_proto_array(value: &burdock::ArrayNd) -> FieldEvaluation {
    // TODO use shape and axes
    match value.to_owned().data.unwrap() {
        burdock::array_nd::Data::Bytes(x) => FieldEvaluation::Bytes(Array1::from(x).into_dyn()),
        burdock::array_nd::Data::Bool(x) => FieldEvaluation::Bool(Array1::from(x.data).into_dyn()),
        burdock::array_nd::Data::I64(x) => FieldEvaluation::I64(Array1::from(x.data).into_dyn()),
        burdock::array_nd::Data::F64(x) => FieldEvaluation::F64(Array1::from(x.data).into_dyn()),
        burdock::array_nd::Data::String(x) => FieldEvaluation::Str(Array1::from(x.data).into_dyn()),
    }
}

pub fn serialize_proto_array(evaluation: &FieldEvaluation) -> burdock::ArrayNd {

    match evaluation {
        FieldEvaluation::Bytes(x) => burdock::ArrayNd {
            datatype: burdock::DataType::Bytes as i32,
            data: Some(burdock::array_nd::Data::Bytes(x.iter().map(|s| *s).collect())),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        FieldEvaluation::Bool(x) => burdock::ArrayNd {
            datatype: burdock::DataType::Bool as i32,
            data: Some(burdock::array_nd::Data::Bool(burdock::Array1Dbool {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        FieldEvaluation::I64(x) => burdock::ArrayNd {
            datatype: burdock::DataType::I64 as i32,
            data: Some(burdock::array_nd::Data::I64(burdock::Array1Di64 {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        FieldEvaluation::F64(x) => burdock::ArrayNd {
            datatype: burdock::DataType::F64 as i32,
            data: Some(burdock::array_nd::Data::F64(burdock::Array1Df64 {
                data: x.iter().map(|s| *s).collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
        FieldEvaluation::Str(x) => burdock::ArrayNd {
            datatype: burdock::DataType::String as i32,
            data: Some(burdock::array_nd::Data::String(burdock::Array1Dstr {
                data: x.iter().cloned().collect()
            })),
            order: (1..x.ndim()).map(|x| {x as u64}).collect(),
            shape: x.shape().iter().map(|y| {*y as u64}).collect()
        },
    }
}
