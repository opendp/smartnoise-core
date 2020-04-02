//! The Whitenoise rust validator contains methods for evaluating and constructing 
//! differentially private analyses.
//! 
//! The validator defines a set of statically checkable properties that are 
//! necessary for a differentially private analysis, and then checks that the submitted analysis
//! satisfies the properties.
//!
//! The validator also takes simple components from the Whitenoise runtime and combines them 
//! into more complex mechanisms.

// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;

#[doc(hidden)]
pub mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {}
}

#[doc(hidden)]
pub use errors::*;
// trait which holds `display_chain`

pub mod base;
pub mod utilities;
pub mod components;
pub mod ffi;
// import all trait implementations
use crate::components::*;
use itertools::Itertools;
use std::collections::HashMap;
use crate::utilities::serial::serialize_value_properties;

// include protobuf-generated traits
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/whitenoise.rs"));
}

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
    request: &proto::RequestValidateAnalysis
) -> Result<proto::response_validate_analysis::Validated> {
    let analysis = request.analysis.clone()
        .ok_or_else(|| Error::from("analysis must be defined"))?;
    let release = request.release.clone()
        .ok_or_else(|| Error::from("release must be defined"))?;

    utilities::propagate_properties(&analysis, &release)?;

    Ok(proto::response_validate_analysis::Validated {
        value: true,
        message: "The analysis is valid.".to_string(),
    })
}


/// Compute overall privacy usage of an analysis.
///
/// The privacy usage is sum of the privacy usages for each node.
/// The Release's actual privacy usage, if defined, takes priority over the maximum allowable privacy usage defined in the Analysis.
pub fn compute_privacy_usage(
    request: &proto::RequestComputePrivacyUsage
) -> Result<proto::PrivacyUsage> {
    let analysis = request.analysis.as_ref()
        .ok_or_else(|| Error::from("analysis must be defined"))?;
    let release = request.release.as_ref()
        .ok_or_else(|| Error::from("release must be defined"))?;

    let (_graph_properties, graph) = utilities::propagate_properties(analysis, release)?;

    let usage_option = graph.iter()
        // return the privacy usage from the release, else from the analysis
        .filter_map(|(node_id, component)| utilities::get_component_privacy_usage(component, release.values.get(node_id)))
        // linear sum
        .fold1(|usage_1, usage_2| utilities::privacy_usage_reducer(
            &usage_1, &usage_2, &|l, r| l + r));

    // TODO: this should probably return a proto::PrivacyUsage with zero based on the privacy definition
    usage_option
        .ok_or_else(|| Error::from("no information is released; privacy usage is none"))
}


/// Generate a json string with a summary/report of the Analysis and Release
pub fn generate_report(
    request: &proto::RequestGenerateReport
) -> Result<String> {
    let analysis = request.analysis.as_ref()
        .ok_or_else(|| Error::from("analysis must be defined"))?;
    let release = request.release.as_ref()
        .ok_or_else(|| Error::from("release must be defined"))?;

    let graph = analysis.computation_graph.to_owned()
        .ok_or("the computation graph must be defined in an analysis")?
        .value;

    let (graph_properties, _graph_expanded) = utilities::propagate_properties(analysis, release)?;
    let release = utilities::serial::parse_release(&release)?;

    let release_schemas = graph.iter()
        .map(|(node_id, component)| {
            let public_arguments = utilities::get_input_arguments(&component, &release)?;
            let input_properties = utilities::get_input_properties(&component, &graph_properties)?;
            // ignore nodes without released values
            let node_release = match release.get(node_id) {
                Some(node_release) => node_release,
                None => return Ok(None)
            };
            component.variant.as_ref()
                .ok_or_else(|| Error::from("component variant must be defined"))?
                .summarize(
                &node_id,
                &component,
                &public_arguments,
                &input_properties,
                &node_release)
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
    request: &proto::RequestAccuracyToPrivacyUsage
) -> Result<proto::PrivacyUsages> {
    let component: &proto::Component = request.component.as_ref()
        .ok_or_else(|| Error::from("component must be defined"))?;
    let privacy_definition: &proto::PrivacyDefinition = request.privacy_definition.as_ref()
        .ok_or_else(|| Error::from("privacy definition must be defined"))?;
    let properties: HashMap<String, base::ValueProperties> = request.properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();
    let accuracies: &proto::Accuracies = request.accuracies.as_ref()
        .ok_or_else(|| Error::from("accuracies must be defined"))?;

    // TODO: expand component and prop accuracy
    Ok(proto::PrivacyUsages {
        values: component.variant.as_ref()
            .ok_or_else(|| Error::from("component variant must be defined"))?
            .accuracy_to_privacy_usage(privacy_definition, &properties, accuracies)?.unwrap()
    })
}


/// Estimate the accuracy of the release of a component, based on a privacy usage.
///
/// No context about the analysis is necessary, just the properties of the arguments of the component.
pub fn privacy_usage_to_accuracy(
    request: &proto::RequestPrivacyUsageToAccuracy
) -> Result<proto::Accuracies> {
    let component: &proto::Component = request.component.as_ref()
        .ok_or_else(|| Error::from("component must be defined"))?;
    let privacy_definition: &proto::PrivacyDefinition = request.privacy_definition.as_ref()
        .ok_or_else(|| Error::from("privacy definition must be defined"))?;
    let properties: HashMap<String, base::ValueProperties> = request.properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();

    Ok(proto::Accuracies {
        values: component.variant.as_ref()
            .ok_or_else(|| Error::from("component variant must be defined"))?
            .privacy_usage_to_accuracy(privacy_definition, &properties, &request.alpha)?.unwrap()
    })
}

pub fn get_properties(
    request: &proto::RequestGetProperties
) -> Result<proto::GraphProperties> {
    let (properties, _graph) = utilities::propagate_properties(
        request.analysis.as_ref()
            .ok_or_else(|| Error::from("analysis must be defined"))?,
        request.release.as_ref()
            .ok_or_else(|| Error::from("release must be defined"))?,
    )?;

    Ok(proto::GraphProperties {
        properties: properties.iter()
            .map(|(node_id, properties)| (*node_id, serialize_value_properties(properties)))
            .collect::<HashMap<u32, proto::ValueProperties>>()
    })
}


/// Expand a component that may be representable as smaller components, and propagate its properties.
///
/// This is function may be called interactively from the runtime as the runtime executes the computational graph, to allow for dynamic graph validation.
/// This is opposed to statically validating a graph, where the nodes in the graph that are dependent on the releases of mechanisms cannot be known and validated until the first release is made.
pub fn expand_component(
    request: &proto::RequestExpandComponent
) -> Result<proto::ComponentExpansion> {
    _expand_component(
        request.privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy definition must be defined"))?,
        request.component.as_ref()
            .ok_or_else(|| Error::from("component must be defined"))?,
        &request.properties.clone(),
        &request.arguments.iter()
            .map(|(k, v)| Ok((k.to_owned(), utilities::serial::parse_value(&v)?)))
            .collect::<Result<_>>()?,
        &request.component_id,
        &request.maximum_id)
}

#[doc(hidden)]
pub fn _expand_component(
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    properties: &HashMap<String, proto::ValueProperties>,
    public_arguments: &HashMap<String, base::Value>,
    component_id: &u32,
    maximum_id: &u32,
) -> Result<proto::ComponentExpansion> {
    let mut properties: base::NodeProperties = properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();

    for (k, v) in public_arguments {
        properties.insert(k.clone(), utilities::inference::infer_property(v)?);
    }

    let result = component.variant.as_ref()
        .ok_or_else(|| Error::from("component variant must be defined"))?.expand_component(
        privacy_definition,
        component,
        &properties,
        component_id,
        maximum_id,
    ).chain_err(|| format!("at node_id {:?}", component_id))?;

    let mut patch_properties = result.properties;
    if result.traversal.is_empty() {
        let propagated_property = component.clone().variant.as_ref()
            .ok_or_else(|| Error::from("component variant must be defined"))?
            .propagate_property(&privacy_definition, &public_arguments, &properties)
            .chain_err(|| format!("at node_id {:?}", component_id))?;

        patch_properties.insert(component_id.to_owned(), utilities::serial::serialize_value_properties(&propagated_property));
    }

    Ok(proto::ComponentExpansion {
        computation_graph: result.computation_graph,
        properties: patch_properties,
        releases: result.releases,
        traversal: result.traversal,
    })
}