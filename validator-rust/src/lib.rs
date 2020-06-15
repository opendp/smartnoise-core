//! The Whitenoise rust validator contains methods for evaluating and constructing
//! differentially private analyses.
//!
//! The validator defines a set of statically checkable properties that are
//! necessary for a differentially private analysis, and then checks that the submitted analysis
//! satisfies the properties.
//!
//! The validator also takes simple components from the Whitenoise runtime and combines them
//! into more complex mechanisms.
//!
//! - [Top-level documentation](https://opendifferentialprivacy.github.io/whitenoise-core/)

#![warn(unused_extern_crates)]
#![allow(clippy::implicit_hasher)]

// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;

#[doc(hidden)]
pub mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}

#[derive(Debug)]
pub struct Warnable<T: std::fmt::Debug>(T, Vec<Error>);
impl<T: std::fmt::Debug> Warnable<T> {
    pub fn new(value: T) -> Self {
        Warnable(value, Vec::new())
    }
}
impl<T: std::fmt::Debug> From<T> for Warnable<T> {
    fn from(value: T) -> Self {
        Warnable::new(value)
    }
}

#[doc(hidden)]
pub use errors::*;
// trait which holds `display_chain`

pub mod base;
pub mod bindings;
pub mod utilities;
pub mod components;
pub mod docs;

// import all trait implementations
use crate::components::*;
use std::collections::{HashMap, HashSet};
use crate::base::{Value, IndexKey, ValueProperties};
use std::iter::FromIterator;
use crate::utilities::privacy::compute_graph_privacy_usage;
use indexmap::map::IndexMap;

// include protobuf-generated traits
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/whitenoise.rs"));
}

#[macro_use] extern crate indexmap;
// define the useful macro for building hashmaps globally
#[macro_export]
#[doc(hidden)]
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         #[allow(unused_mut)]
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

/// Validate if an analysis is well-formed.
///
/// Checks that the graph is a DAG.
/// Checks that static properties are met on all components.
///
/// Useful for static validation of an analysis.
/// Since some components require public arguments, mechanisms that depend on other mechanisms cannot be verified until the components they depend on have been validated.
///
/// The system may also be run dynamically- prior to expanding each node, calling the expand_component endpoint will also validate the component being expanded.
/// NOTE: Evaluating the graph dynamically opens up additional potential timing attacks.
pub fn validate_analysis(
    privacy_definition: Option<proto::PrivacyDefinition>,
    mut computation_graph: HashMap<u32, proto::Component>,
    mut release: base::Release
) -> Result<()> {
    utilities::propagate_properties(
        &privacy_definition,
        &mut computation_graph,
        &mut release,
        None,
        false)?;
    Ok(())
}


/// Compute overall privacy usage of an analysis.
///
/// The privacy usage is sum of the privacy usages for each node.
/// The Release's actual privacy usage, if defined, takes priority over the maximum allowable privacy usage defined in the Analysis.
pub fn compute_privacy_usage(
    privacy_definition: proto::PrivacyDefinition,
    mut computation_graph: HashMap<u32, proto::Component>,
    mut release: base::Release
) -> Result<proto::PrivacyUsage> {

    let properties = utilities::propagate_properties(
        &Some(privacy_definition.clone()),
        &mut computation_graph,
        &mut release, None, false)?.0;

    let privacy_usage = compute_graph_privacy_usage(
        &computation_graph, &privacy_definition, &properties, &release)?;

    utilities::privacy::privacy_usage_check(&privacy_usage, None, false)?;

    Ok(privacy_usage)
}


/// Generate a json string with a summary/report of the Analysis and Release
pub fn generate_report(
    privacy_definition: proto::PrivacyDefinition,
    mut computation_graph: HashMap<u32, proto::Component>,
    mut release: base::Release
) -> Result<String> {

    let graph_properties = utilities::propagate_properties(
        &Some(privacy_definition),
        &mut computation_graph,
        &mut release, None, false)?.0;

    // variable names
    let mut nodes_varnames: HashMap<u32, Vec<IndexKey>> = HashMap::new();

    utilities::get_traversal(&computation_graph)?.iter().map(|node_id| {
        let component: proto::Component = computation_graph.get(&node_id).unwrap().to_owned();
        let public_arguments = utilities::get_public_arguments(&component, &release)?;

        // variable names for argument nodes
        let mut arguments_vars: IndexMap<base::IndexKey, Vec<IndexKey>> = IndexMap::new();

        // iterate through argument nodes
        for (field_id, field) in &component.arguments() {
            // get variable names corresponding to that argument
            if let Some(arg_vars) = nodes_varnames.get(field) {
                arguments_vars.insert(field_id.clone(), arg_vars.clone());
            }
        }

        // get variable names for this node
        let node_vars = component.get_names(
            &public_arguments,
            &arguments_vars,
            release.get(node_id).map(|v| v.value.clone()).as_ref());

        // update names in indexmap
        node_vars.map(|v| nodes_varnames.insert(node_id.clone(), v)).ok();

        Ok(())
    }).collect::<Result<()>>()
        // ignore any error- still generate the report even if node names could not be derived
        .ok();

    let release_schemas = computation_graph.iter()
        .map(|(node_id, component)| {
            let public_arguments = utilities::get_public_arguments(&component, &release)?;
            let input_properties = utilities::get_input_properties(&component, &graph_properties)?;
            let variable_names = nodes_varnames.get(&node_id);
            // ignore nodes without released values
            let node_release = match release.get(node_id) {
                Some(node_release) => node_release.value.clone(),
                None => return Ok(None)
            };
            component.summarize(
                *node_id,
                &component,
                &public_arguments,
                &input_properties,
                &node_release,
                variable_names,
            )
        })
        .collect::<Result<Vec<Option<Vec<utilities::json::JSONRelease>>>>>()?.into_iter()
        .filter_map(|v| v).flat_map(|v| v)
        .collect::<Vec<utilities::json::JSONRelease>>();

    match serde_json::to_string(&release_schemas) {
        Ok(serialized) => Ok(serialized),
        Err(_) => Err("unable to parse report into json".into())
    }
}


/// Estimate the privacy usage necessary to bound accuracy to a given value.
///
/// No context about the analysis is necessary, just the privacy definition and properties of the arguments of the component.
pub fn accuracy_to_privacy_usage(
    component: proto::Component,
    privacy_definition: proto::PrivacyDefinition,
    properties: IndexMap<IndexKey, base::ValueProperties>,
    accuracies: proto::Accuracies
) -> Result<proto::PrivacyUsages> {

    let proto_properties = component.arguments().iter()
        .filter_map(|(name, idx)| Some((*idx, properties.get(name)?.clone())))
        .collect::<HashMap<u32, base::ValueProperties>>();

    let mut computation_graph = hashmap![
        component.arguments().values().max().cloned().unwrap_or(0) + 1 => component
    ];

    let (properties, _) = utilities::propagate_properties(
        &Some(privacy_definition.clone()),
            &mut computation_graph,
        &mut HashMap::new(),
        Some(proto_properties),
        false,
    )?;

    let privacy_usages = computation_graph.iter().map(|(idx, component)| {
        let component_properties = component.arguments().iter()
            .filter_map(|(name, idx)| Some((name.clone(), properties.get(idx)?.clone())))
            .collect::<IndexMap<base::IndexKey, base::ValueProperties>>();

        Ok(match component.accuracy_to_privacy_usage(
            &privacy_definition, &component_properties, &accuracies)? {
            Some(accuracies) => Some((*idx, accuracies)),
            None => None
        })
    })
        .collect::<Result<Vec<Option<(u32, Vec<proto::PrivacyUsage>)>>>>()?
        .into_iter().filter_map(|v| v)
        .collect::<HashMap<u32, Vec<proto::PrivacyUsage>>>();

    Ok(proto::PrivacyUsages {
        values: privacy_usages.into_iter().map(|(_, v)| v).collect::<Vec<Vec<proto::PrivacyUsage>>>()
            .first()
            .ok_or_else(|| Error::from("privacy usage is not defined"))?.clone()
    })
}


/// Estimate the accuracy of the release of a component, based on a privacy usage.
///
/// No context about the analysis is necessary, just the properties of the arguments of the component.
pub fn privacy_usage_to_accuracy(
    component: proto::Component,
    privacy_definition: proto::PrivacyDefinition,
    properties: IndexMap<IndexKey, base::ValueProperties>,
    alpha: f64
) -> Result<proto::Accuracies> {

    let proto_properties = component.arguments().iter()
        .filter_map(|(name, idx)| Some((*idx, properties.get(name)?.clone())))
        .collect::<HashMap<u32, base::ValueProperties>>();

    let mut computation_graph = hashmap![
        component.arguments().values().max().cloned().unwrap_or(0) + 1 => component
    ];

    let (properties, _) = utilities::propagate_properties(
        &Some(privacy_definition.clone()),
        &mut computation_graph,
        &mut HashMap::new(),
        Some(proto_properties),
        false,
    )?;

    let accuracies = computation_graph.iter().map(|(idx, component)| {
        let component_properties = component.arguments().iter()
            .filter_map(|(name, idx)| Some((name.clone(), properties.get(idx)?.clone())))
            .collect::<IndexMap<IndexKey, base::ValueProperties>>();

        Ok(match component.privacy_usage_to_accuracy(
            &privacy_definition, &component_properties, alpha)? {
            Some(accuracies) => Some((*idx, accuracies)),
            None => None
        })
    })
        .collect::<Result<Vec<Option<(u32, Vec<proto::Accuracy>)>>>>()?
        .into_iter().filter_map(|v| v)
        .collect::<HashMap<u32, Vec<proto::Accuracy>>>();

    Ok(proto::Accuracies {
        values: accuracies.into_iter().map(|(_, v)| v).collect::<Vec<Vec<proto::Accuracy>>>()
            // TODO: propagate/combine accuracies, don't just take the first
            .first()
            .ok_or_else(|| Error::from("accuracy is not defined"))?.clone()
    })
}

/// Expand a component that may be representable as smaller components, and propagate its properties.
///
/// This is function may be called interactively from the runtime as the runtime executes the computational graph, to allow for dynamic graph validation.
/// This is opposed to statically validating a graph, where the nodes in the graph that are dependent on the releases of mechanisms cannot be known and validated until the first release is made.
pub fn expand_component(
    component: proto::Component,
    mut properties: IndexMap<IndexKey, ValueProperties>,
    public_arguments: IndexMap<IndexKey, base::ReleaseNode>,
    privacy_definition: Option<proto::PrivacyDefinition>,
    component_id: u32,
    maximum_id: u32,
) -> Result<base::ComponentExpansion> {

    for (k, v) in &public_arguments {
        if !v.public {
            return Err("private data should not be sent to the validator".into())
        }
        properties.insert(k.clone(),
                          utilities::inference::infer_property(
                              &v.value,
                              properties.get(k))?);
    }

    let mut result = component.expand_component(
        &privacy_definition,
        &component,
        &properties,
        component_id,
        maximum_id,
    ).chain_err(|| format!("at node_id {:?}", component_id))?;

    let public_values = public_arguments.iter()
        .map(|(name, release_node)| (name.clone(), &release_node.value))
        .collect::<IndexMap<IndexKey, &Value>>();

    if result.traversal.is_empty() {
        let Warnable(propagated_property, propagation_warnings) = component
            .propagate_property(&privacy_definition, &public_values, &properties, component_id)
            .chain_err(|| format!("at node_id {:?}", component_id))?;

        result.warnings.extend(propagation_warnings.into_iter()
            .map(|err| err.chain_err(|| format!("at node_id {:?}", component_id))));
        result.properties.insert(component_id.to_owned(), propagated_property);
    }

    Ok(result)
}


/// Retrieve the static properties from every reachable node on the graph.
pub fn get_properties(
    privacy_definition: Option<proto::PrivacyDefinition>,
    mut computation_graph: HashMap<u32, proto::Component>,
    mut release: base::Release,
    node_ids: Vec<u32>
) -> Result<(HashMap<u32, ValueProperties>, Vec<Error>)> {

    if !node_ids.is_empty() {
        let mut ancestors = HashSet::<u32>::new();
        let mut traversal = Vec::from_iter(node_ids.into_iter());
        while !traversal.is_empty() {
            let node_id = traversal.pop().unwrap();
            if let Some(component) = computation_graph.get(&node_id){
                component.arguments().values().for_each(|v| traversal.push(*v))
            }
            ancestors.insert(node_id);
        }
        computation_graph = computation_graph.iter()
            .filter(|(idx, _)| ancestors.contains(idx))
            .map(|(idx, component)| (*idx, component.clone()))
            .collect::<HashMap<u32, proto::Component>>();
        release = release.iter()
            .filter(|(idx, _)| ancestors.contains(idx))
            .map(|(idx, release_node)|
                (*idx, release_node.clone()))
            .collect();
    }

    // don't return all properties- only those in the original graph
    let keep_ids = HashSet::<u32>::from_iter(computation_graph.keys().cloned());

    let (mut properties, warnings) = utilities::propagate_properties(
        &privacy_definition, &mut computation_graph,
        &mut release, None, true,
    )?;

    properties.retain(|node_id, _| keep_ids.contains(node_id));
    Ok((properties, warnings))
}
