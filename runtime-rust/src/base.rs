extern crate yarrow_validator;

use yarrow_validator::proto;
use yarrow_validator::utilities::buffer;
use yarrow_validator::utilities::graph as yarrow_graph;

use ndarray::prelude::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::iter::FromIterator;

use crate::components;


pub fn execute_graph(analysis: &proto::Analysis,
                     release: &proto::Release,
                     dataset: &proto::Dataset) -> Result<proto::Release, String> {
    let node_ids_release: HashSet<u32> = yarrow_graph::get_release_nodes(&analysis)?;

    // stack for storing which nodes to evaluate next
    let mut traversal = Vec::new();
    traversal.extend(yarrow_graph::get_sinks(&analysis).into_iter());

    let mut evaluations = buffer::release_to_evaluations(release)?;

    let graph: &HashMap<u32, proto::Component> = &analysis.graph;

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

            let evaluation = execute_component(
                &graph.get(&node_id).unwrap(), &evaluations, &dataset)?;

            evaluations.insert(node_id, evaluation);

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
    buffer::evaluations_to_release(&evaluations)
}

pub fn execute_component(component: &proto::Component,
                         evaluations: &buffer::GraphEvaluation,
                         dataset: &proto::Dataset) -> Result<buffer::NodeEvaluation, String> {
    let arguments = buffer::get_arguments(&component, &evaluations);

    use proto::component::Value as Value;
    match component.to_owned().value.unwrap() {
        Value::Literal(x) => components::component_literal(&x),
        Value::Datasource(x) => components::component_datasource(&x, &dataset, &arguments),
        Value::Add(x) => components::component_add(&x, &arguments),
        Value::Subtract(x) => components::component_subtract(&x, &arguments),
        Value::Divide(x) => components::component_divide(&x, &arguments),
        Value::Multiply(x) => components::component_multiply(&x, &arguments),
        Value::Power(x) => components::component_power(&x, &arguments),
        Value::Negate(x) => components::component_negate(&x, &arguments),
        Value::Bin(x) => components::component_bin(&x, &arguments),
        Value::Rowmin(x) => components::component_row_wise_min(&x, &arguments),
        Value::Rowmax(x) => components::component_row_wise_max(&x, &arguments),
        // Value::Count(x) => components::component_count(&x, &arguments),
        // Value::Histogram(x) => components::component_histogram(&x, &arguments),
        Value::Mean(x) => components::component_mean(&x, &arguments),
        Value::Median(x) => components::component_median(&x, &arguments),
        Value::Sum(x) => components::component_sum(&x, &arguments),
        Value::Variance(x) => components::component_variance(&x, &arguments),
//        Value::Kthsamplemoment(x) => components::component_kth_sample_moment(&x, &arguments),
        Value::LaplaceMechanism(x) => components::component_laplace_mechanism(&x, &arguments),
        Value::GaussianMechanism(x) => components::component_gaussian_mechanism(&x, &arguments),
        Value::SimpleGeometricMechanism(x) => components::component_simple_geometric_mechanism(&x, &arguments),
        variant => Err(format!("Component type not implemented: {:?}", variant))
    }
}
