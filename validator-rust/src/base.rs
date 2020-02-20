use crate::proto;
use itertools::Itertools;

use crate::utilities;
use crate::components;
use crate::components::Component;

use std::collections::HashMap;
use crate::utilities::constraint::Constraint;
use crate::hashmap;


pub fn validate_analysis(
    analysis: &proto::Analysis
) -> Result<proto::response_validate_analysis::Validated, String> {
    // check if acyclic
    let traversal = utilities::graph::get_traversal(analysis)?;

    // TODO: check that there is at most one Materialize
    // TODO: check shapes and lengths (to prevent leaking from errors)
    return Ok(proto::response_validate_analysis::Validated { value: true, message: "The analysis is valid.".to_string() });
}

pub fn compute_privacy_usage(
    analysis: &proto::Analysis, release: &proto::Release,
) -> Result<proto::PrivacyUsage, String> {
    let graph: &HashMap<u32, proto::Component> = &analysis.graph;

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

pub fn expand_graph(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<proto::Analysis, String> {
    let graph_constraints: utilities::constraint::GraphConstraint
        = utilities::constraint::propagate_constraints(analysis, release)?;

    let mut graph: HashMap<u32, proto::Component> = analysis.graph.to_owned();

    let mut max_node_id = match graph.keys().fold1(std::cmp::max) {
        Some(x) => *x,
        // the graph is empty, and empty graphs are trivially fully expanded
        None => return Ok(analysis.to_owned())
    };

    let graph_keys: Vec<u32> = graph.keys().map(|&x: &u32| x.clone()).collect();
    // expand each component in the graph
    for node_id in graph_keys {
        let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
        let result = component.clone().value.unwrap().expand_graph(
            &analysis.privacy_definition.to_owned().unwrap(),
            &component,
            max_node_id,
            node_id,
            &utilities::constraint::get_constraints(&component, &graph_constraints),
        )?;
        max_node_id = result.0;
        graph.extend(result.1);
    }

    Ok(proto::Analysis {
        graph: graph,
        privacy_definition: analysis.privacy_definition.clone(),
    })
}

// TODO: create report json
pub fn generate_report(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<String, String> {
    return Ok("{\"key\": \"value\"}".to_owned());
}