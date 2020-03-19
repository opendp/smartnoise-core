use whitenoise_validator::errors::*;


extern crate whitenoise_validator;

use whitenoise_validator::proto;
use whitenoise_validator::utilities::serial;

use crate::components::*;

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use itertools::Itertools;

use whitenoise_validator::base::{get_input_properties, Value, ValueProperties};
use whitenoise_validator::utilities::inference::infer_property;
use whitenoise_validator::utilities::serial::{serialize_value_properties, parse_release};

pub type NodeArguments<'a> = HashMap<String, &'a Value>;

/// Given a description of computation, and some computed values, execute the computation and return computed values
///
/// The analysis is a directed acyclic graph.
/// - vertices are components (a unit of computation)
/// - edges are arguments
///
/// When a component is executed, the output of the node is stored in the release
/// When the graph completes execution, the release is filtered and returned
///
/// # Arguments
/// * `analysis` - a computational graph and definition of privacy, in prost protobuf format
/// * `release` - a collection of precomputed values for components in the graph
///
/// # Return
/// a collection of computed values for components in the graph
pub fn execute_graph(analysis: &proto::Analysis,
                     release: &proto::Release,
) -> Result<proto::Release> {

    // stack for storing which nodes to evaluate next
    let computation_graph = analysis.computation_graph.to_owned().unwrap();
    let mut traversal: Vec<u32> = get_sinks(&computation_graph).into_iter().collect();

    let mut release = serial::parse_release(release)?;

    let mut graph: HashMap<u32, proto::Component> = computation_graph.value;
    let mut graph_properties: HashMap<u32, proto::ValueProperties> = HashMap::new();
    let mut maximum_id = graph.keys()
        .fold1(std::cmp::max)
        .map(|x| x.clone())
        .unwrap_or(0);

    // TEMP FIX FOR UNEVALUATED PROPERTIES
    for (node_id, value) in release.clone() {
        graph_properties.insert(node_id.clone(), serialize_value_properties(&infer_property(&value)?));
    }

    // track node parents. Each key is a node id, and the value is the set of node ids that use it
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents.entry(*source_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();

        if release.contains_key(&node_id) {
            traversal.pop();
            continue;
        }

        let component: proto::Component = graph.get(&node_id).unwrap().clone();
        let arguments = component.to_owned().arguments;

        // discover if any dependencies remain uncomputed
        let mut evaluable = true;
        for source_node_id in arguments.values() {
            if !release.contains_key(&source_node_id) {
                evaluable = false;
                traversal.push(*source_node_id);
                break;
            }
        }

        if !evaluable {
            continue;
        }

        let node_properties: HashMap<String, proto::ValueProperties> =
            get_input_properties(&component, &graph_properties)?;

        let public_arguments = node_properties.iter()
            .filter(|(_k, v)| match v.variant.clone().unwrap() {
                proto::value_properties::Variant::Arraynd(v) => v.releasable,
                _ => false
            })
            .map(|(k, _v)| (k.clone(), release
                .get(component.arguments.get(k).unwrap()).unwrap().clone()))
            .collect::<HashMap<String, Value>>();

//        println!("expanding component {:?}", component);
//        println!("public arguments {:?}", public_arguments);
//        println!("node properties {:?}", node_properties);

        // all arguments have been computed, attempt to expand the current node
        let expansion: proto::ComponentExpansion = whitenoise_validator::base::expand_component(
            &analysis.privacy_definition.to_owned().unwrap(),
            &component,
            &node_properties,
            &public_arguments,
            node_id,
            maximum_id,
        )?;

        graph.extend(expansion.computation_graph.clone());
        graph_properties.extend(expansion.properties);
        release.extend(parse_release(&proto::Release{values: expansion.releases})?);
        traversal.extend(expansion.traversal.clone());

        maximum_id = *expansion.computation_graph.keys()
            .max().map(|v| v.max(&maximum_id)).unwrap_or(&maximum_id);

        if !expansion.traversal.is_empty() {
            continue;
        }

        traversal.pop();

        // the patch may have overwritten the current component
        let component = graph.get(&node_id).unwrap();

        let mut node_arguments = NodeArguments::new();
        component.arguments.iter().for_each(|(field_id, field)| {
            let evaluation = release.get(&field).unwrap();
            node_arguments.insert(field_id.to_owned(), evaluation);
        });

//        println!("Evaluating node_id {:?}, {:?}", node_id, component.variant);
        let evaluation = component.to_owned().variant.unwrap().evaluate(&node_arguments)?;

        release.insert(node_id, evaluation);

        // prune arguments from the release
        for argument_node_id in arguments.values() {
            if let Some(parent_node_ids) = parents.get_mut(argument_node_id) {
                parent_node_ids.remove(&node_id);

                // remove argument node from release if all children evaluated, and is private or omitted
                if parent_node_ids.len() == 0 {
                    let releasable = match graph_properties.get(argument_node_id) {
                        Some(properties) => match properties.variant.clone().unwrap() {
                            proto::value_properties::Variant::Arraynd(v) => v.releasable,
                            _=> false
                        },
                        None => false
                    };
                    let argument_component = graph.get(argument_node_id).clone().unwrap();

                    if argument_component.omit || !releasable {
                        release.remove(argument_node_id);
                    }
                }
            }
        }
    }

    // ensure that the only keys remaining in the release are releasable and not omitted
    for node_id in release.to_owned().keys() {
        let releasable = match graph_properties.get(node_id) {
            Some(properties) => match properties.variant.clone().unwrap() {
                proto::value_properties::Variant::Arraynd(v) => v.releasable,
                _ => false
            },
            None => false
        };

        match graph.get(node_id) {
            Some(component) => if component.omit || !releasable {
                release.remove(node_id);
            },
            // delete node ids in the release that are not present in the graph
            None => {
                release.remove(node_id);
            }
        }
    }
    serial::serialize_release(&release)
}

/// Retrieve the set of node ids in a ComputationGraph that have no dependent nodes.
///
/// # Arguments
/// * `computation_graph` - a prost protobuf hashmap representing a computation graph
///
/// # Returns
/// The set of node ids that have no dependent nodes
pub fn get_sinks(computation_graph: &proto::ComputationGraph) -> HashSet<u32> {
    let mut node_ids = HashSet::<u32>::new();
    // start with all nodes
    for node_id in computation_graph.value.keys() {
        node_ids.insert(*node_id);
    }

    // remove nodes that are referenced in arguments
    for node in computation_graph.value.values() {
        for source_node_id in node.arguments.values() {
            node_ids.remove(&source_node_id);
        }
    }

    // move to heap, transfer ownership to caller
    return node_ids.to_owned();
}
