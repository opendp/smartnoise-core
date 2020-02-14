use ::yarrow_validator::yarrow as yarrow;

use crate::utilities;
use std::collections::HashMap;

pub fn validate_analysis(analysis: &yarrow::Analysis) -> yarrow::Validated {
    // check if acyclic
    let traversal = utilities::graph::get_traversal(analysis)?;

    // TODO: check that there is at most one Materialize
    // TODO: check shapes and lengths (to prevent leaking from errors)
    return yarrow::Validated {valid: true}
}

pub fn compute_privacy_usage(analysis: &yarrow::Analysis, release: &yarrow::Release) -> yarrow::PrivacyUsage {
    let traversal = utilities::graph::get_traversal(analysis);
    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    graph.iter()
        // optionally extract the minimum usage between the analysis and release
        .map(|node_id, component| get_privacy_usage(component, release.get(node_id)))
        // ignore nodes without privacy usage
        .filter(|privacy_usage| privacy_usage.is_some())
        // linear sum
        .fold1(|usage_1, usage_2| privacy_usage_reducer(
            usage_1?, usage_2?, &|l, r| l + r))
}

pub fn get_privacy_usage(
    component: &yarrow::Component,
    release_node_option: Option<&yarrow::ReleaseNode>
) -> Option<yarrow::PrivacyUsage> {
    let privacy_usage = match component.to_owned().value? {
        yarrow::component::Value::DPSum(x) => Some(x.privacy_usage),
        yarrow::component::Value::DPCount(x) => Some(x.privacy_usage),
        yarrow::component::Value::DPMean(x) => Some(x.privacy_usage),
        yarrow::component::Value::DPVariance(x) => Some(x.privacy_usage),
        yarrow::component::Value::DPMomentRaw(x) => Some(x.privacy_usage),
        yarrow::component::Value::DPVariance(x) => Some(x.privacy_usage),
        _ => None
    };

    if privacy_usage.is_none() {
        return None;
    }

    if let Some(release_node) = release_node_option {
        return privacy_usage_reducer(
            privacy_usage?,
            release_node.privacy_usage,
            &|l, r| std::cmp::min(l, r));
    }
    privacy_usage
}

pub fn privacy_usage_reducer(
    left: &yarrow::PrivacyUsage,
    right: &yarrow::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64
) -> yarrow::PrivacyUsage {

    match left.usage.to_owned()? {
        yarrow::privacy_usage::Usage::distance_pure(x) => {
            if let yarrow::privacy_usage::Usage::distance_pure(y) = right.usage {
                return yarrow::PrivacyUsage {
                    usage: Some(yarrow::privacy_usage::DistancePure {
                        epsilon: operator(x.epsilon, y.epsilon)
                    })
                }
            }
            None
        },
        yarrow::privacy_usage::Usage::distance_approximate(x) => {
            if let yarrow::privacy_usage::Usage::distance_approximate(y) = right.usage {
                return yarrow::PrivacyUsage {
                    usage: Some(yarrow::privacy_usage::DistanceApproximate {
                        epsilon: operator(x.epsilon, y.epsilon),
                        delta: operator(x.delta, y.delta)
                    })
                }
            }
            None
        }
    }
}

pub fn expand_graph(
    analysis: &yarrow::Analysis
) -> yarrow::Analysis {
    let mut graph: HashMap<u32, yarrow::Component> = analysis.graph;
    let mut extension: HashMap<u32, yarrow::Component> = HashMap::new();
    let mut max_node_id = graph.keys().iter().fold1(std::cmp::max);

    graph.keys().for_each(|node_id| {
        let result = expand_component(
            graph.get(node_id).unwrap(),
            *node_id,
            max_node_id
        );
        max_node_id = result.0;
        extension.extend(result.1);
    });

    yarrow::Analysis {
        graph: Some(graph.extend(extension)),
        privacy_definition: None
    }
}

pub fn expand_component(
    component: &yarrow::Component,
//    constraint: &yarrow::Constraint,
    component_id: u32,
    minimum_id: u32
) -> (u32, HashMap<u32, yarrow::Component>) {
    let mut current_id = minimum_id.clone();
    let arguments = component.arguments;

    let mut graph: HashMap<u32, yarrow::Component> = HashMap::new();

    match component.to_owned().value? {
        yarrow::component::Value::Mean(x) => {
            // impute
            current_id += 1;
            let id_impute = current_id.clone();
            graph.insert(id_sum, yarrow::Component {
                arguments: Some(hashmap!["data" => component.arguments.get("data")]),
                value: Some(yarrow::component::Value::Impute(yarrow::Impute {})),
                omit: Some(true), batch: None
            });
            // sum
            current_id += 1;
            let id_sum = current_id.clone();
            graph.insert(id_sum, yarrow::Component {
                arguments: Some(hashmap!["data" => id_impute]),
                value: Some(yarrow::component::Value::DPSum(yarrow::DPSum {})),
                omit: Some(true), batch: None
            });
            // count
            current_id += 1;
            let id_count = current_id.clone();
            graph.insert(id_count, yarrow::Component {
                arguments: Some(hashmap!["data" => id_impute]),
                value: Some(yarrow::component::Value::DPCount(yarrow::DPCount {})),
                omit: Some(true), batch: None
            });
            // divide
            graph.insert(component_id, yarrow::Component {
                arguments: Some(hashmap!["left" => id_sum, "right" => id_count]),
                value: Some(yarrow::component::Value::Divide(yarrow::Divide {})),
                omit: Some(true), batch: None
            });

            expand_graph(yarrow::Analysis {
                graph: Some(graph),
                privacy_definition: None
            }).graph
        },
        _ => hashmap![component_id => component.to_owned()]
    }
}