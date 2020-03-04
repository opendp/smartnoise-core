use yarrow_validator::errors::*;
use yarrow_validator::ErrorKind::{PrivateError, PublicError};

extern crate yarrow_validator;

use yarrow_validator::{proto};
use yarrow_validator::utilities::{graph as yarrow_graph, serial};

use crate::components::*;

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use itertools::Itertools;

use yarrow_validator::base::{get_input_properties, Value};
use yarrow_validator::utilities::inference::infer_property;
use yarrow_validator::utilities::serial::serialize_properties;

pub type NodeArguments<'a> = HashMap<String, &'a Value>;

pub fn execute_graph(analysis: &proto::Analysis,
                     release: &proto::Release) -> Result<proto::Release> {
    let node_ids_release: HashSet<u32> = yarrow_graph::get_release_nodes(&analysis)?;

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(yarrow_graph::get_sinks(&analysis).into_iter());

    let mut evaluations = serial::parse_release(release)?;

    let mut graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value;
    let mut graph_properties: HashMap<u32, proto::Properties> = HashMap::new();
    let mut maximum_id = graph.keys()
        .fold1(std::cmp::max)
        .map(|x| x.clone())
        .unwrap_or(0);

    // TEMP FIX FOR UNEVALUATED PROPERTIES
    for (node_id, value) in evaluations.clone() {
        graph_properties.insert(node_id.clone(), serialize_properties(&infer_property(&value)?));
    }

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents.entry(*source_node_id).or_insert_with(HashSet::<u32>::new).insert(*node_id);
        })
    });

    while !traversal.is_empty() {
        let node_id: u32 = *traversal.last().unwrap();

        if evaluations.contains_key(&node_id) {
            traversal.pop();
            continue;
        }

        let component: proto::Component = graph.get(&node_id).unwrap().clone();
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

        if !evaluable {
            continue;
        }

        let node_properties: HashMap<String, proto::Properties> =
            get_input_properties(&component, &graph_properties)?;

        let public_arguments = node_properties.iter()
            .filter(|(_k, v)| v.releasable)
            .map(|(k, _v)| (k.clone(), evaluations
                .get(component.arguments.get(k).unwrap()).unwrap().clone()))
            .collect::<HashMap<String, Value>>();

//        println!("expanding component {:?}", component);
//        println!("public arguments {:?}", public_arguments);
//        println!("node properties {:?}", node_properties);
        // all arguments have been computed, attempt to expand the current node
        let expansion: proto::response_expand_component::ExpandedComponent = yarrow_validator::base::expand_component(
            &analysis.privacy_definition.to_owned().unwrap(),
            &component,
            &node_properties,
            &public_arguments,
            node_id,
            maximum_id
        )?;

        graph_properties.insert(node_id, expansion.properties.unwrap());
        graph.extend(expansion.computation_graph.unwrap().value);

        if maximum_id != expansion.maximum_id {
            maximum_id = expansion.maximum_id;
            continue
        }

        traversal.pop();


        let mut node_arguments = NodeArguments::new();
        component.arguments.iter().for_each(|(field_id, field)| {
            let evaluation = evaluations.get(&field).unwrap();
            node_arguments.insert(field_id.to_owned(), evaluation);
        });

        println!("Evaluating {:?}", node_id);
        let evaluation = component.to_owned().value.unwrap().evaluate(&node_arguments)?;

        evaluations.insert(node_id, evaluation);

        // remove references to parent node, and if empty and private
        for argument_node_id in arguments.values() {
            if let Some(temporary_node) = parents.get_mut(argument_node_id) {
                temporary_node.remove(&node_id);
            }
            if let Some(argument_node) = parents.get(argument_node_id) {
                if !node_ids_release.contains(argument_node_id) {
                    evaluations.remove(argument_node_id);
                    // parents.remove(argument_node_id); // optional
                }
            }
        }
    }
    serial::serialize_release(&evaluations)
}
