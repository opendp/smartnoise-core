use arrow::record_batch::RecordBatch;

use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;

use std::sync::Arc;
use std::iter::FromIterator;

use arrow::array::{ArrayRef, PrimitiveArray};

use crate::utilities;

// Include the `items` module, which is generated from items.proto.
pub mod burdock {
    include!(concat!(env!("OUT_DIR"), "/burdock.rs"));
}

type FieldEvaluation = ArrayRef;
type NodeEvaluation = HashMap<String, FieldEvaluation>;
type GraphEvaluation = HashMap<u32, NodeEvaluation>;

pub fn get_argument(graph_evaluation: &GraphEvaluation, argument: &burdock::component::Field) -> FieldEvaluation {
    graph_evaluation.get(&argument.source_node_id).unwrap().get(&argument.source_field).unwrap().to_owned()
}

pub fn get_release_nodes(analysis: &burdock::Analysis) -> HashSet<u32> {

    let mut release_node_ids = HashSet::<u32>::new();
    let sink_node_ids = get_sinks(analysis);

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
    for node_id in analysis.graph.keys() {
        node_ids.insert(*node_id);
    }

    for node in analysis.graph.values() {
        for field in node.arguments.values() {
            node_ids.remove(&field.source_node_id);
        }
    }

    return node_ids.to_owned();
}

//pub fn get_sources(analysis: &burdock::Analysis) -> HashSet<u32> {
//    let mut node_ids = HashSet::<u32>::new();
//    for (node_id, node) in &analysis.graph {
//        if node.arguments.len() > 0 {continue;}
//        node_ids.insert(*node_id);
//    }
//    return node_ids.to_owned();
//}

pub fn is_privatizer(component: &burdock::Component) -> bool {
    use burdock::component::Value::*;
    match component.to_owned().value.unwrap() {
        Dpmeanlaplace(_x) => true,
        _ => false
    }
}

pub fn execute_graph(analysis: &burdock::Analysis,
                     release: &burdock::Release,
                     data: &RecordBatch) -> burdock::Release {

    let node_ids_release: HashSet<u32> = get_release_nodes(&analysis);

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(get_sinks(&analysis).into_iter());
//    for node_id in get_sinks(&analysis) { traversal.push(node_id); }

    let mut evaluations = release_to_evaluations(release);
    let graph: &HashMap<u32, burdock::Component> = &analysis.graph;

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    for (node_id, component) in graph {
        for field in component.arguments.values() {
            let argument_node_id = &field.source_node_id;
            parents.entry(*argument_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        }
    }

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();
        let component = graph.get(&node_id).unwrap();
        let arguments = component.to_owned().arguments;

        // discover if any dependencies remain uncomputed
        let mut evaluable = true;
        for field in arguments.values() {
            if !evaluations.contains_key(&field.source_node_id) {
                evaluable = false;
                break;
            }
        }

        // check if all arguments are available
        if evaluable {
            traversal.pop();

            evaluations.insert(node_id, execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, data));

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
                         data: &RecordBatch) -> NodeEvaluation {

    match component.to_owned().value.unwrap() {
        burdock::component::Value::Datasource(x) => {
            println!("datasource");

            // https://docs.rs/datafusion-arrow/0.12.0/arrow/array/trait.Array.html
            let (index, _schema) = data.schema().column_with_name(&x.column_id).unwrap();
            let mut evaluation = NodeEvaluation::new();
            evaluation.insert("data".to_owned(), data.column(index).to_owned());
            evaluation
        },
        burdock::component::Value::Add(_x) => {
            println!("add");

            let left_generic = get_argument(
                &evaluations,
                &component.arguments.get("left").unwrap());
            let left = left_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

            let right_generic = get_argument(
                &evaluations,
                &component.arguments.get("right").unwrap());
            let right = right_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

            let mut evaluation = NodeEvaluation::new();
            let data = arrow::compute::add(&left, &right).unwrap();
            evaluation.insert("data".to_owned(), Arc::new(data));
            evaluation
        },
        burdock::component::Value::Dpmeanlaplace(_x) => {
            println!("dpmeanlaplace");

            let data_generic = get_argument(
                &evaluations,
                &component.arguments.get("data").unwrap());
            let data = data_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

            let min_generic = get_argument(
                &evaluations,
                &component.arguments.get("minimum").unwrap());
            let min = min_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);

            let max_generic = get_argument(
                &evaluations,
                &component.arguments.get("maximum").unwrap());
            let max = max_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);

            let num_records_generic = get_argument(
                &evaluations,
                &component.arguments.get("num_records").unwrap());
            let num_records = num_records_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);

            let epsilon_generic = get_argument(
                &evaluations,
                &component.arguments.get("epsilon").unwrap());
            let epsilon = epsilon_generic.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);

            let mut evaluation = NodeEvaluation::new();
            // TODO: don't use len, pull from arguments
            let mean = arrow::compute::sum(&data).unwrap() / data.len() as f64;
            let sensitivity = (max - min) / num_records;
            let noised = mean + utilities::sample_laplace(0., sensitivity / epsilon);
            let result: PrimitiveArray<arrow::datatypes::Float64Type> = vec![noised].into();

            evaluation.insert("data".to_owned(), Arc::new(result));
            evaluation
        },
        _ => NodeEvaluation::new()
    }
}

pub fn release_to_evaluations(release: &burdock::Release) -> GraphEvaluation {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        let mut evaluations_node = NodeEvaluation::new();
        for (field_id, field_release) in &node_release.values {
//            burdock::Value {
//
//                data: Some(burdock::value::Data::ScalarNumeric(23.2))
//            };
            let numeric = match field_release.data.to_owned().unwrap() {
                burdock::value::Data::ScalarNumeric(x) => x,
                _ => 0.23
            };

            let result: PrimitiveArray<arrow::datatypes::Float64Type> = vec![numeric].into();
            evaluations_node.insert(field_id.to_owned(), Arc::new(result));
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
            let temp = field_eval.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(0);
            let value = burdock::Value {
                datatype: 1, // burdock::DataType::ScalarNumeric,
                data: Some(burdock::value::Data::ScalarNumeric(temp))
            };

            node_release.insert(field_name.to_owned(), value);
        }
        releases.insert(*node_id, burdock::ReleaseNode {
            values: node_release.to_owned()
        });
    }
    burdock::Release {
        values: releases
    }
}