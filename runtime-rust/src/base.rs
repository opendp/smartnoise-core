use whitenoise_validator::errors::*;


extern crate whitenoise_validator;

use whitenoise_validator::proto;
use whitenoise_validator::utilities::{serial, get_input_properties};

use crate::components::*;

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use itertools::Itertools;

use whitenoise_validator::base::{Value, ReleaseNode};
use whitenoise_validator::utilities::serial::{parse_release};
use std::iter::FromIterator;

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
/// * `filter_level` - configure the amount of information included in the return
///
/// # Return
/// a collection of computed values for components in the graph
pub fn execute_graph(
    analysis: &proto::Analysis,
    release: &proto::Release,
    filter_level: &proto::FilterLevel
) -> Result<proto::Release> {

    // stack for storing which nodes to evaluate next
    let computation_graph = analysis.computation_graph.to_owned()
        .ok_or_else(|| Error::from("computation_graph must be defined to execute an analysis"))?;
    let mut graph: HashMap<u32, proto::Component> = computation_graph.value;

    // core state for the graph execution algorithm
    let mut traversal: Vec<u32> = get_sinks(&graph).into_iter().collect();
    let mut graph_properties: HashMap<u32, proto::ValueProperties> = HashMap::new();
    let mut release = serial::parse_release(release)?;
    let mut maximum_id = graph.keys()
        .fold1(std::cmp::max)
        .map(|x| x.clone())
        .unwrap_or(0);

    // values in the passed release are kept in the release
    // TODO: consider requesting properties from the validator for nodes that already exist in the release
    let preserve_ids: HashSet<u32> = HashSet::from_iter(release.keys().cloned());

    // track node parents. Each key is a node id, and the value is the set of node ids that use it
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents.entry(*source_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    // evaluate components until the traversal is empty
    while !traversal.is_empty() {

        let node_id: u32 = *traversal.last().unwrap();

        // skip the node if it has already been evaluated
        if release.contains_key(&node_id) {
            traversal.pop();
            continue;
        }

        let component: proto::Component = graph.get(&node_id)
            .ok_or_else(|| Error::from("attempted to retrieve a non-existent component id"))?.clone();

        // check if any dependencies of the current node remain unevaluated
        let mut evaluable = true;
        for source_node_id in component.arguments.values() {
            if !release.contains_key(&source_node_id) {
                evaluable = false;
                traversal.push(*source_node_id);
                break;
            }
        }

        if !evaluable {
            continue;
        }

        // all dependencies are present in the graph. Begin node expansion

        // collect metadata about node inputs
        let node_properties: HashMap<String, proto::ValueProperties> =
            get_input_properties(&component, &graph_properties)?;
        let public_arguments = component.arguments.iter()
            .filter(|(_, node_id)| preserve_ids.contains(node_id) || is_public(node_id, &graph, &graph_properties, true) && release.contains_key(node_id))
            .map(|(name, node_id)| (name.clone(), release.get(node_id).unwrap()))
            .filter(|(_, release_node)| release_node.public)
            .map(|(name, release_node)| (name, release_node.value.clone()))
            .collect::<HashMap<String, Value>>();

        // expand the current node
        let expansion: proto::ComponentExpansion = whitenoise_validator::_expand_component(
            &analysis.privacy_definition.to_owned()
                .ok_or_else(|| Error::from("privacy_definition must be defined"))?,
            &component,
            &node_properties,
            &public_arguments,
            &node_id,
            &maximum_id,
        )?;

        // extend the runtime state with the expansion
        graph.extend(expansion.computation_graph.clone());
        graph_properties.extend(expansion.properties);
        release.extend(parse_release(&proto::Release{values: expansion.releases})?);
        traversal.extend(expansion.traversal.clone());

        maximum_id = *expansion.computation_graph.keys()
            .max().map(|v| v.max(&maximum_id)).unwrap_or(&maximum_id);

        // if nodes were added to the traversal, then evaluate the new nodes first
        if !expansion.traversal.is_empty() {
            continue;
        }

        // no nodes were added to the traversal. Begin node execution
        traversal.pop();

        // the expansion may have overwritten the current component
        let component = graph.get(&node_id).unwrap();

        // collect arguments by string name to the component that will be executed
        let node_arguments = component.arguments.iter()
            .map(|(name, node_id)| (name.clone(), &release.get(node_id).unwrap().value))
            .collect::<HashMap<String, &Value>>();

        // evaluate the component using the Evaluable trait, which is implemented on the proto::component::Variant enum
        let evaluation = component.clone().variant
            .ok_or_else(|| Error::from("variant of component must be known"))?
            .evaluate(&node_arguments)?;

        // store the evaluated `Value` enum in the release
        release.insert(node_id, evaluation);

        if filter_level != &proto::FilterLevel::All {
            // prune evaluations from the release. Private nodes that have no unevaluated parents do not need be stored anymore
            for argument_node_id in component.arguments.values() {
                if let Some(parent_node_ids) = parents.get_mut(argument_node_id) {
                    parent_node_ids.remove(&node_id);

                    // remove argument node from release if all children evaluated, and is private or omitted
                    if parent_node_ids.len() == 0 && !(preserve_ids.contains(argument_node_id) || is_public(argument_node_id, &graph, &graph_properties, false)) {
                        release.remove(argument_node_id);
                    }
                }
            }
        }
    }

    // apply the filtering level to the final release
    serial::serialize_release(&match filter_level {
        proto::FilterLevel::Public => release.into_iter()
            .filter(|(node_id, _)| is_public(node_id, &graph, &graph_properties, false))
            .collect::<HashMap<u32, ReleaseNode>>(),
        proto::FilterLevel::PublicAndPrior => release.into_iter()
            .filter(|(node_id, _)| preserve_ids.contains(node_id) || is_public(node_id, &graph, &graph_properties, false))
            .collect::<HashMap<u32, ReleaseNode>>(),
        proto::FilterLevel::All => release,
    })

}

/// Retrieve the set of node ids in a graph that have no dependent nodes.
///
/// # Arguments
/// * `computation_graph` - a prost protobuf hashmap representing a computation graph
///
/// # Returns
/// The set of node ids that have no dependent nodes
pub fn get_sinks(computation_graph: &HashMap<u32, proto::Component>) -> HashSet<u32> {
    // start with all nodes
    let mut node_ids = HashSet::from_iter(computation_graph.keys().cloned());

    // remove nodes that are referenced in arguments
    computation_graph.values()
        .for_each(|component| component.arguments.values()
            .for_each(|source_node_id| {
                node_ids.remove(source_node_id);
            }));

    return node_ids;
}


pub fn is_public(
    node_id: &u32,
    graph: &HashMap<u32, proto::Component>,
    graph_properties: &HashMap<u32, proto::ValueProperties>,
    allow_omitted: bool
) -> bool {

    // component must be known
    let component = match graph.get(node_id) {
        Some(component) => component,
        None => return false
    };

    // component must not be omitted
    if !allow_omitted && component.omit {
        return false;
    }


    // if the component is a literal, it is public if not marked private
    if let proto::component::Variant::Literal(literal) = component.variant.clone().unwrap() {
        return !literal.private;
    }

    // otherwise use the properties to determine if public
    match graph_properties.get(node_id) {
        Some(property) => match property.variant.clone().unwrap() {
            proto::value_properties::Variant::Array(v) => v.releasable,
            proto::value_properties::Variant::Jagged(v) => v.releasable,
            proto::value_properties::Variant::Hashmap(_) => false
        },
        None => false
    }
}