//! The SmartNoise rust runtime is an execution engine for evaluating differentially private analyses.
//! 
//! The runtime contains implementations of basic data transformations and aggregations, 
//! statistics, and privatizing mechanisms. These functions are combined in the 
//! SmartNoise validator to create more complex differentially private analyses.
//!
//! - [Top-level documentation](https://opendp.github.io/smartnoise-core/)

#![warn(unused_extern_crates)]
// s! macros for slicing from ndarray use unsafe code
#![deny(unsafe_code)]
#![allow(clippy::implicit_hasher)]

pub use smartnoise_validator::proto;
use smartnoise_validator::errors::*;

pub mod utilities;
pub mod components;
pub mod base;

use std::collections::{HashMap, HashSet};
use std::vec::Vec;

use smartnoise_validator::base::{Value, ReleaseNode, Release, IndexKey, ComponentExpansion, ValueProperties};
use smartnoise_validator::utilities::{get_sinks, get_input_properties, get_dependents};

use crate::components::Evaluable;

use std::iter::FromIterator;
use indexmap::map::IndexMap;


pub type NodeArguments = IndexMap<IndexKey, Value>;


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
pub fn release(
    privacy_definition: Option<proto::PrivacyDefinition>,
    mut computation_graph: HashMap<u32, proto::Component>,
    mut release: Release,
    filter_level: proto::FilterLevel
) -> Result<(Release, Vec<Error>)> {

    if let Some(privacy_definition) = &privacy_definition {
        if !cfg!(feature="use-mpfr") && privacy_definition.protect_floating_point {
            return Err("runtime has been compiled without mpfr, and floating point protections have been enabled".into())
        }
    }

    // core state for the graph execution algorithm
    let mut traversal: Vec<u32> = get_sinks(&computation_graph).into_iter().collect();

    // derive properties for any private nodes in the release
    let (mut properties, mut warnings) = smartnoise_validator::get_properties(
        privacy_definition.clone(),
        computation_graph.clone(),
        release.clone(),
        release.keys().copied().collect()
    )?;

    let mut maximum_id = computation_graph.keys().max().cloned().unwrap_or(0);

    // for if the filtering level is set to retain values
    let original_ids: HashSet<u32> = HashSet::from_iter(release.keys().cloned());

    // track node parents. Each key is a node id, and the value is the set of node ids that use it
    let mut parents = get_dependents(&computation_graph);

    // evaluate components until the traversal is empty
    while !traversal.is_empty() {

        let component_id: u32 = *traversal.last().unwrap();

        // skip the node if it has already been evaluated
        if release.contains_key(&component_id) {
            traversal.pop();
            continue;
        }

        let component: &proto::Component = computation_graph.get(&component_id)
            .ok_or_else(|| Error::from("attempted to retrieve a non-existent component id"))?;

        // check if any dependencies of the current node remain unevaluated
        let mut evaluable = true;
        for source_node_id in component.arguments().values() {
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
        let node_properties: IndexMap<IndexKey, ValueProperties> =
            get_input_properties(&component, &properties)?;
        let public_arguments = component.arguments().into_iter()
            .map(|(name, node_id)| (name, release.get(&node_id).unwrap()))
            .filter(|(_, release_node)| release_node.public)
            .map(|(name, release_node)| (name, release_node.clone()))
            .collect::<IndexMap<IndexKey, ReleaseNode>>();

        // expand the current node
        let mut expansion: ComponentExpansion = match smartnoise_validator::expand_component(
            component.clone(),
            node_properties,
            public_arguments,
            privacy_definition.clone(),
            component_id,
            maximum_id) {
            Ok(expansion) => expansion,
            Err(err) => {
                warnings.push(err);
                // continue without evaluating the faulty component or any parents
                let mut descendant_traversal = Vec::new();
                let mut descendants = HashSet::new();
                descendant_traversal.push(component_id);
                while !descendant_traversal.is_empty() {
                    let descendant = descendant_traversal.pop().unwrap();
                    if let Some(parents) = parents.get(&descendant) {
                        parents.iter().for_each(|parent| {
                            descendant_traversal.push(*parent);
                        })
                    }
                    descendants.insert(descendant);
                }
                traversal = traversal.into_iter()
                    .filter(|v| !descendants.contains(v))
                    .collect();
                continue
            }
        };

        maximum_id = expansion.computation_graph.keys()
            .max().cloned().unwrap_or(0).max(maximum_id);

        // extend the runtime state with the expansion
        computation_graph.extend(expansion.computation_graph);
        properties.extend(expansion.properties);
        release.extend(expansion.releases);
        warnings.extend(expansion.warnings);

        // if nodes were added to the traversal, then evaluate the new nodes first
        if !expansion.traversal.is_empty() {
            expansion.traversal.reverse();
            traversal.extend(expansion.traversal);

            // TODO: this could be more optimized
            parents = get_dependents(&computation_graph);
            continue;
        }

        // no nodes were added to the traversal. Begin node execution
        traversal.pop();

        // the expansion may have overwritten the current component
        let component = computation_graph.get(&component_id).unwrap();

        // println!("node id:    {:?}", component_id);
        // println!("component:  {:?}", component.variant);
        // println!("arguments:  {:?}", node_arguments);

        let mut node_arguments = IndexMap::<IndexKey, Value>::new();
        for (name, argument_node_id) in component.arguments().into_iter() {

            // if keeping all, then all arguments must be copied because all arguments are retained
            if filter_level == proto::FilterLevel::All {
                release.get(&argument_node_id)
                    .map(|v| v.clone().value)
                    .or_else(|| node_arguments.get(&name).cloned())
                    .map(|release_node|
                        node_arguments.insert(name, release_node));
                continue
            }

            // all parent nodes have been evaluated, so orphans are no longer needed for further calculation
            let is_orphan = if let Some(parent_node_ids) = parents.get_mut(&argument_node_id) {
                parent_node_ids.remove(&component_id);
                parent_node_ids.is_empty()
            } else {true};

            // true if the node was in the prior release, and we are retaining prior nodes
            let must_include = filter_level == proto::FilterLevel::PublicAndPrior
                && original_ids.contains(&argument_node_id);

            // public nodes are always kept in the release
            let is_public = release.get(&argument_node_id).map(|v| v.public).unwrap_or(false);

            // omitted nodes are side-effects of graph expansions that may be removed
            let is_omitted = computation_graph.get(&argument_node_id)
                .map(|v| v.omit).unwrap_or(true);

            // remove argument node from release
            if is_orphan && ((!must_include && !is_public) || is_omitted) {
                release.remove(&argument_node_id)
            } else {
                release.get(&argument_node_id).cloned()
            }
                .map(|v| v.value)
                .or_else(|| node_arguments.get(&name).cloned())
                .map(|v| node_arguments.insert(name, v));
        }

        // evaluate the component using the Evaluable trait, which is implemented on the proto::component::Variant enum
        let mut evaluation = component.variant.as_ref()
            .ok_or_else(|| Error::from("variant of component must be known"))?
            .evaluate(&privacy_definition, node_arguments)?;

        // println!("evaluation: {:?}", evaluation);

        evaluation.public = properties.get(&component_id)
            .map(ValueProperties::is_public)
            .unwrap_or(false);

        // store the evaluated `Value` enum in the release
        release.insert(component_id, evaluation);
    }

    // remove all omitted nodes (temporarily added to the graph while executing)
    release.retain(|node_id, _| !computation_graph.get(node_id)
        .map(|v| v.omit)
        .unwrap_or(true));

    // apply the filtering level to the final release
    match filter_level {

        proto::FilterLevel::Public =>
            release.retain(|_, release_node|
                release_node.public),

        proto::FilterLevel::PublicAndPrior =>
            release.retain(|node_id, release_node|
                release_node.public || original_ids.contains(node_id)),

        proto::FilterLevel::All => (),
    };

    Ok((release, warnings))
}
