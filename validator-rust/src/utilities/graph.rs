use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;

pub fn get_traversal(
    analysis: &yarrow::Analysis
) -> Result<std::Vec<u32>, &'static str> {

    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents.entry(*source_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue = graph.iter()
        .filter(|(node_id, component)| component.arguments.is_empty())
        .map(|(node_id, component)| node_id).collect();

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last()?;
        queue.pop();
        traversal.push(queue_node_id);

        let mut is_cyclic = false;

        parents.get(&queue_node_id)?.iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id)?.to_owned().arguments;

            // if parent has been reached more times than it has arguments, then it is cyclic
            visited.entry(parent_node_id).or_insert(0) += 1;
            if visited.get(parent_node_id)? > parent_arguments.len() {
                is_cyclic = true;
            }

            // check that all arguments of parent_node have been evaluated before adding to queue
            if parent_arguments.values().all(|argument_node_id| traversal.includes(argument_node_id)) {
                queue.push(parent_node_id);
            }
        });

        if is_cyclic {
            return Err("Graph is cyclic.")
        }

    }
    return Ok(traversal);
}

pub fn get_unevaluated(
    analysis: &yarrow::Analysis,
    release: &yarrow::Release
) -> Result<std::HashSet<u32>, &'static str> {

    let sink_node_ids = get_sinks(analysis);

    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    let mut queue = get_sinks(&analysis).into_iter().collect();
    let mut unevaluated = HashSet::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last()?;
        queue.pop();
        traversal.push(queue_node_id);

        let arguments = graph.get(&queue_node_id)?.to_owned().arguments;
        arguments.keys()
            .filter(|argument_node_id| !release.contains(argument_node_id))
            .for_each(|argument_node_id| {
                unevaluated.insert(argument_node_id);
                queue.insert(argument_node_id);
            });
    }
    Ok(unevaluated)
}

pub fn get_release_nodes(analysis: &yarrow::Analysis) -> Result<HashSet<u32>, &'static str> {

    let mut release_node_ids = HashSet::<u32>::new();
    // assume sinks are private
    let sink_node_ids = get_sinks(analysis);
//    println!("sink nodes: {:?}", sink_node_ids);

    // traverse back through arguments until privatizers found
    let mut node_queue = VecDeque::from_iter(sink_node_ids.iter());

    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    while !node_queue.is_empty() {
        let node_id = node_queue.pop_front()?;
        let component = graph.get(&node_id)?;

        if !component.omit {
            release_node_ids.insert(*node_id);
        }
        else {
            for source_node_id in component.arguments.values() {
                node_queue.push_back(&source_node_id);
            }
        }
    }

    return Ok(release_node_ids);
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