use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;

use ndarray::prelude::*;
use arrow::record_batch::RecordBatch;
use std::iter::FromIterator;

// Include the `items` module, which is generated from items.proto.
pub mod burdock {
    include!(concat!(env!("OUT_DIR"), "/burdock.rs"));
}

type FieldEvaluation = arrow::record_batch::RecordBatch;
type NodeEvaluation = HashMap<String, FieldEvaluation>;
type GraphEvaluation = HashMap<u32, NodeEvaluation>;

pub fn get_argument(graph_evaluation: GraphEvaluation, argument: burdock::component::Field) -> FieldEvaluation {
    graph_evaluation.get(&argument.source_node_id).unwrap().get(&argument.source_field).unwrap().to_owned()
}

// TODO: implement
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
            for (fieldName, field) in &component.arguments {
                node_queue.push_back(&field.source_node_id);
            }
        }
    }

    return release_node_ids;
}

pub fn get_sinks(analysis: &burdock::Analysis) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    for (node_id, node) in &analysis.graph {
        node_ids.insert(*node_id);
    }

    for (node_id, node) in &analysis.graph {
        for (argument_name, field) in &node.arguments {
            node_ids.remove(&field.source_node_id);
        }
    }

    return node_ids.to_owned();
}

pub fn get_sources(analysis: &burdock::Analysis) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    for (node_id, node) in &analysis.graph {
        if node.arguments.len() > 0 {continue;}
        node_ids.insert(*node_id);
    }
    return node_ids.to_owned();
}

pub fn is_privatizer(component: &burdock::Component) -> bool {
    use burdock::component::Value::*;
    match component.to_owned().value.unwrap() {
        Dpmeanlaplace(x) => true,
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
        for (fieldName, field) in &component.arguments {
            let argument_node_id = &field.source_node_id;
            parents.entry(*argument_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        }
    }

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();
        let component = graph.get(&node_id).unwrap();
        let arguments = component.to_owned().arguments;

        // discover if any dependencies remain uncomputed
        let mut evaluable = false;
        let mut i = 0;
        for (fieldName, field) in &arguments {
            if !evaluations.contains_key(&field.source_node_id) {
                evaluable = false;
                break;
            }
            i += 1;
        }

        // check if all arguments are available
        if &i == &arguments.len() {
            traversal.pop();

            evaluations.insert(node_id, execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, data));

            // remove references to parent node, and if empty and private
            for (argumentName, argument) in &arguments {
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
//    use burdock::component::Value;
//    match component.to_owned().value.unwrap() {
//        Value::Add(x) => {
//            println!("test add");
//        },
//        _ => false
//    }
    NodeEvaluation::new()
}

pub fn release_to_evaluations(release: &burdock::Release) -> GraphEvaluation {
    GraphEvaluation::new()
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> burdock::Release {
    burdock::Release {
        values: HashMap::new()
    }
}