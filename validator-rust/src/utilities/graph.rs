use crate::errors::*;


use std::collections::{HashMap, HashSet};

use crate::proto;

pub fn get_traversal(
    analysis: &proto::Analysis
) -> Result<Vec<u32>> {

    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().unwrap().value;

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

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments.is_empty())
        .map(|(node_id, _component)| node_id.to_owned()).collect();

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        let mut is_cyclic = false;

        parents.get(&queue_node_id).unwrap().iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id).unwrap().to_owned().arguments;

            // if parent has been reached more times than it has arguments, then it is cyclic
            let count = visited.entry(*parent_node_id).or_insert(0);
            *count += 1;
            if visited.get(parent_node_id).unwrap() > &parent_arguments.len() {
                is_cyclic = true;
            }

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
