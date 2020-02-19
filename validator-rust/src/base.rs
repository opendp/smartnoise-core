use crate::proto;
use itertools::Itertools;

use crate::utilities;
use crate::components;

use std::collections::HashMap;
use crate::hashmap;

#[derive(Clone, Debug)]
pub struct Constraint<T> {
    pub nullity: bool,
    pub is_releasable: bool,
    pub min: Option<T>,
    pub max: Option<T>,
    pub categories: Option<Vec<T>>,
    pub num_records: Option<i32>,
}

pub fn validate_analysis(
    analysis: &proto::Analysis
) -> Result<proto::response_validate_analysis::Validated, &'static str> {
    // check if acyclic
    let traversal = utilities::graph::get_traversal(analysis)?;

    // TODO: check that there is at most one Materialize
    // TODO: check shapes and lengths (to prevent leaking from errors)
    return Ok(proto::response_validate_analysis::Validated {value: true, message: "The analysis is valid.".to_string()})
}

pub fn compute_privacy_usage(
    analysis: &proto::Analysis, release: &proto::Release
) -> Result<proto::PrivacyUsage, &'static str> {
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
        None => Err("no information is released; privacy usage is none")
    }
}

pub fn get_component_privacy_usage(
    component: &proto::Component,
    release_node: Option<&proto::ReleaseNode>
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
    operator: &dyn Fn(f64, f64) -> f64
) -> proto::PrivacyUsage {

    use proto::privacy_usage::Usage as Usage;

    proto::PrivacyUsage {
        usage: match (left.usage.to_owned().unwrap(), right.usage.to_owned().unwrap()) {
            (Usage::DistancePure(x), Usage::DistancePure(y)) => Some(Usage::DistancePure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Usage::DistanceApproximate(x), Usage::DistanceApproximate(y)) => Some(Usage::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta)
            })),
            _ => None
        }
    }
}

// TODO: implement constraint propagation
pub fn propagate_constraints(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<HashMap<u32, proto::Constraint>, &'static str> {
    Ok(hashmap![])
}

pub fn expand_graph(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<proto::Analysis, &'static str> {
    match propagate_constraints(analysis, release) {
        Ok(constraints) => expand_graph_recursive(analysis, &constraints),
        Err(e) => Err(e)
    }
}

pub fn expand_graph_recursive(
    analysis: &proto::Analysis,
    constraints: &HashMap<u32, proto::Constraint>
) -> Result<proto::Analysis, &'static str> {

    let mut graph: HashMap<u32, proto::Component> = analysis.graph.to_owned();
    let mut extension: HashMap<u32, proto::Component> = HashMap::new();
    let max_node_id_option = graph.keys().fold1(std::cmp::max);

    // the graph is empty, and empty graphs are trivially fully expanded
    if max_node_id_option.is_none() {
        return Ok(analysis.to_owned())
    }

    let mut max_node_id = *max_node_id_option.unwrap();

    graph.keys().for_each(|node_id| {
        let result = expand_component(
            graph.get(node_id).unwrap(),
            &analysis.privacy_definition.to_owned().unwrap(),
            &proto::Constraint{},
            *node_id,
            max_node_id
        );
        max_node_id = result.0;
        extension.extend(result.1);
    });

    graph.extend(extension);
    Ok(proto::Analysis {
        graph: graph,
        privacy_definition: None
    })
}

// TODO: insert component expansions
pub fn expand_component(
    component: &proto::Component,
    privacy_definition: &proto::PrivacyDefinition,
    constraint: &proto::Constraint,
    component_id: u32,
    maximum_id: u32
) -> (u32, HashMap<u32, proto::Component>) {
    let mut current_id = maximum_id.clone();
    let arguments = component.arguments.to_owned();

    let mut constraints: HashMap<u32, proto::Constraint> = HashMap::new();

    let mut graph: HashMap<u32, proto::Component> = HashMap::new();

    match component.value.to_owned().unwrap() {
        proto::component::Value::Dpmean(x) => {
            // mean
            current_id += 1;
            let id_mean = current_id.clone();
            graph.insert(id_mean, proto::Component {
                arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
                value: Some(proto::component::Value::Mean(proto::Mean {})),
                omit: true, batch: component.batch
            });
            // noising
            graph.insert(component_id, proto::Component {
                arguments: hashmap!["data".to_owned() => id_mean],
                value: Some(proto::component::Value::LaplaceMechanism(proto::LaplaceMechanism {
                    privacy_usage: x.privacy_usage,
                    sensitivity: compute_sensitivity(component, constraint, privacy_definition)
                })),
                omit: true, batch: component.batch
            });

            (current_id, expand_graph_recursive(&proto::Analysis {
                graph,
                privacy_definition: None
            }, &constraints).unwrap().graph)
        },
        _ => (maximum_id, hashmap![component_id => component.to_owned()])
    }
}

// TODO: insert sensitivity derivations
pub fn compute_sensitivity(
    component: &proto::Component,
    constraint: &proto::Constraint,
    privacy_definition: &proto::PrivacyDefinition
) -> f64 {
    1.
}

// TODO: create report json
pub fn generate_report(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<String, &'static str> {
    return Ok("{\"key\": \"value\"}".to_owned());
}

// TODO: insert accuracy to privacy_usage derivations
pub fn accuracy_to_privacy_usage(
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    constraint: &proto::Constraint,
    accuracy: &proto::Accuracy
) -> Result<proto::PrivacyUsage, &'static str> {
    Ok(proto::PrivacyUsage {
        usage: Some(proto::privacy_usage::Usage::DistancePure(proto::privacy_usage::DistancePure {
            epsilon: 0.
        }))
    })
}

// TODO: insert privacy_usage to accuracy derivations
pub fn privacy_usage_to_accuracy(
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    constraint: &proto::Constraint
) -> Result<proto::Accuracy, &'static str> {
    Ok(proto::Accuracy {value: 12.})
}
