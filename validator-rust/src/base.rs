use crate::proto;
use itertools::Itertools;

use crate::utilities;

use crate::components::*;

use std::collections::HashMap;
use crate::utilities::properties::{NodeProperties};


use crate::utilities::serial::{Value, parse_value, serialize_value};
use crate::components::literal::infer_property;
use std::ops::Deref;


// equivalent to proto Release
pub type GraphEvaluation = HashMap<u32, Value>;

// arguments to a node prior to evaluation
pub type NodeArguments<'a> = HashMap<String, &'a Value>;

pub fn get_arguments<'a>(component: &proto::Component, graph_evaluation: &'a GraphEvaluation) -> NodeArguments<'a> {
    let mut arguments = NodeArguments::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: &'a Value = graph_evaluation.get(&field).unwrap();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}

pub fn get_arguments_copy(component: &proto::Component, graph_evaluation: &GraphEvaluation) -> HashMap<String, Value> {
    let mut arguments = HashMap::<String, Value>::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: Value = graph_evaluation.get(&field).unwrap().to_owned();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}

pub fn get_argument(arguments: &NodeArguments, name: &str) -> Result<Value, String> {
    match arguments.get(name) {
        Some(argument) => Ok(argument.deref().to_owned()),
        _ => Err((name.to_string() + " is not defined").to_string())
    }
}

pub fn release_to_evaluations(release: &proto::Release) -> Result<GraphEvaluation, String> {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        evaluations.insert(*node_id, parse_value(&node_release.value.to_owned().unwrap()).unwrap());
    }
    Ok(evaluations)
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> Result<proto::Release, String> {
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();
    for (node_id, node_eval) in evaluations {
        if let Ok(array_serialized) = serialize_value(node_eval) {
            releases.insert(*node_id, proto::ReleaseNode {
                value: Some(array_serialized),
                privacy_usage: None,
            });
        }
    }
    Ok(proto::Release {
        values: releases
    })
}



pub fn validate_analysis(
    analysis: &proto::Analysis
) -> Result<proto::response_validate_analysis::Validated, String> {
    // check if acyclic
    let _traversal = utilities::graph::get_traversal(analysis)?;

    // TODO: check that there is at most one Materialize
    // TODO: check shapes and lengths (to prevent leaking from errors)
    return Ok(proto::response_validate_analysis::Validated { value: true, message: "The analysis is valid.".to_string() });
}

pub fn compute_privacy_usage(
    analysis: &proto::Analysis, release: &proto::Release,
) -> Result<proto::PrivacyUsage, String> {
    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().unwrap().value;

    let usage_option = graph.iter()
        // optionally extract the minimum usage between the analysis and release
        .map(|(node_id, component)| get_component_privacy_usage(component, release.values.get(node_id)))
        // ignore nodes without privacy usage
        .filter(|privacy_usage| privacy_usage.is_some())
        .map(|privacy_usage| privacy_usage.unwrap())
        // linear sum
        .fold1(|usage_1, usage_2| privacy_usage_reducer(
            &usage_1, &usage_2, &|l, r| l + r));

    // TODO: this should probably return a proto::PrivacyUsage with zero based on the privacy definition
    match usage_option {
        Some(x) => Ok(x),
        None => Err("no information is released; privacy usage is none".to_string())
    }
}

pub fn get_component_privacy_usage(
    component: &proto::Component,
    release_node: Option<&proto::ReleaseNode>,
) -> Option<proto::PrivacyUsage> {
    let privacy_usage_option: Option<proto::PrivacyUsage> = match component.to_owned().value? {
        proto::component::Value::Dpsum(x) => x.privacy_usage,
        proto::component::Value::Dpcount(x) => x.privacy_usage,
        proto::component::Value::Dpmean(x) => x.privacy_usage,
        proto::component::Value::Dpvariance(x) => x.privacy_usage,
        proto::component::Value::Dpmomentraw(x) => x.privacy_usage,
        proto::component::Value::Dpvariance(x) => x.privacy_usage,
        _ => None
    };

    if privacy_usage_option.is_none() {
        return None;
    }

    if let Some(release_node) = release_node {
        if let Some(release_node_usage) = &release_node.privacy_usage {
            return Some(privacy_usage_reducer(
                &privacy_usage_option.unwrap(),
                &release_node_usage,
                &|l, r| l.min(r)));
        }
    }
    privacy_usage_option
}

pub fn privacy_usage_reducer(
    left: &proto::PrivacyUsage,
    right: &proto::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64,
) -> proto::PrivacyUsage {
    use proto::privacy_usage::Usage as Usage;

    proto::PrivacyUsage {
        usage: match (left.usage.to_owned().unwrap(), right.usage.to_owned().unwrap()) {
            (Usage::DistancePure(x), Usage::DistancePure(y)) => Some(Usage::DistancePure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Usage::DistanceApproximate(x), Usage::DistanceApproximate(y)) => Some(Usage::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta),
            })),
            _ => None
        }
    }
}

pub fn expand_component(
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    properties: &HashMap<String, proto::Properties>,
    arguments: &HashMap<String, Value>,
    node_id_output: u32,
    node_id_maximum: u32
) -> Result<proto::response_expand_component::ExpandedComponent, String> {
    let mut properties: NodeProperties = properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::properties::Properties::from_proto(&v)))
        .collect();

    for (k, v) in arguments {
        properties.insert(k.clone(), infer_property(&v)?);
    }

    let result = component.clone().value.unwrap().expand_graph(
        privacy_definition,
        component,
        &properties,
        node_id_output,
        node_id_maximum,
    )?;

    let property = component.clone().value.unwrap().propagate_property(arguments, &properties)?;

    Ok(proto::response_expand_component::ExpandedComponent {
        computation_graph: Some(proto::ComputationGraph { value: result.1 }),
        properties: Some(property.to_proto()),
        maximum_id: result.0
    })
}

// TODO: create report json
pub fn generate_report(
    _analysis: &proto::Analysis,
    _release: &proto::Release,
) -> Result<String, String> {
    return Ok("{\"key\": \"value\"}".to_owned());
}