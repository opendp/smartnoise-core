use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
use crate::proto;

pub fn get_traversal(
    analysis: &proto::Analysis
) -> Result<Vec<u32>> {

    println!("PRE");
    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().unwrap().value;

    println!("A");
    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        if !parents.contains_key(node_id) {
            parents.insert(*node_id, HashSet::<u32>::new());
        }
        component.arguments.values().for_each(|argument_node_id| {
            parents.entry(*argument_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });

    println!("B");
    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments.is_empty())
        .map(|(node_id, _component)| node_id.to_owned()).collect();

    println!("C");

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        println!("C.1");

        let mut is_cyclic = false;

        parents.get(&queue_node_id).unwrap().iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id).unwrap().to_owned().arguments;

            // if parent has been reached more times than it has arguments, then it is cyclic
            let count = visited.entry(*parent_node_id).or_insert(0);
            *count += 1;
            println!("C.2");
            if visited.get(parent_node_id).unwrap() > &parent_arguments.len() {
                is_cyclic = true;
            }
            println!("C.3");

            // check that all arguments of parent_node have been evaluated before adding to queue
            if parent_arguments.values().all(|argument_node_id| traversal.contains(argument_node_id)) {
                queue.push(*parent_node_id);
            }
        });

        if is_cyclic {
            return Err("Graph is cyclic.".into())
        }

    }
    return Ok(traversal);
}

pub fn get_unevaluated(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<HashSet<u32>> {

    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().to_owned().unwrap().value;

    let mut traversal: Vec<u32> = Vec::new();
    let mut queue: Vec<u32> = get_sinks(&analysis).into_iter().collect();
    let mut unevaluated = HashSet::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        let arguments = graph.get(&queue_node_id).unwrap().to_owned().arguments;
        arguments.values()
            .filter(|argument_node_id| !release.values.contains_key(argument_node_id))
            .for_each(|argument_node_id| {
                unevaluated.insert(*argument_node_id);
                queue.push(*argument_node_id);
            });
    }
    Ok(unevaluated)
}

pub fn get_release_nodes(analysis: &proto::Analysis) -> Result<HashSet<u32>> {

    let mut release_node_ids = HashSet::<u32>::new();
    // assume sinks are private
    let sink_node_ids = get_sinks(analysis);
//    println!("sink nodes: {:?}", sink_node_ids);

    // traverse back through arguments until privatizers found
    let mut node_queue = VecDeque::from_iter(sink_node_ids.iter());

    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().to_owned().unwrap().value;

    while !node_queue.is_empty() {
        let node_id = node_queue.pop_front().unwrap();
        let component = graph.get(&node_id).unwrap();

        if !component.omit {
            release_node_ids.insert(*node_id);
        }
        else {
            let arguments: &HashMap<String, u32> = &component.arguments;
            for source_node_id in arguments.values() {
                node_queue.push_back(&source_node_id);
            }
        }
    }

    return Ok(release_node_ids);
}

pub fn get_sinks(analysis: &proto::Analysis) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    // start with all nodes
    for node_id in analysis.computation_graph.to_owned().unwrap().value.keys() {
        node_ids.insert(*node_id);
    }

    // remove nodes that are referenced in arguments
    for node in analysis.computation_graph.to_owned().unwrap().value.values() {
        for source_node_id in node.arguments.values() {
            node_ids.remove(&source_node_id);
        }
    }

    // move to heap, transfer ownership to caller
    return node_ids.to_owned();
}