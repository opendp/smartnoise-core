use crate::yarrow;
use itertools::Itertools;

use crate::utilities;
use std::collections::HashMap;

macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

pub fn validate_analysis(
    analysis: &yarrow::Analysis
) -> Result<yarrow::response_validate_analysis::Validated, &'static str> {
    // check if acyclic
    let traversal = utilities::graph::get_traversal(analysis)?;

    // TODO: check that there is at most one Materialize
    // TODO: check shapes and lengths (to prevent leaking from errors)
    return Ok(yarrow::response_validate_analysis::Validated {value: true, message: "The analysis is valid.".to_string()})
}

pub fn compute_privacy_usage(
    analysis: &yarrow::Analysis, release: &yarrow::Release
) -> Result<yarrow::PrivacyUsage, &'static str> {
    let graph: &HashMap<u32, yarrow::Component> = &analysis.graph;

    let usage_option = graph.iter()
        // optionally extract the minimum usage between the analysis and release
        .map(|(node_id, component)| get_component_privacy_usage(component, release.values.get(node_id)))
        // ignore nodes without privacy usage
        .filter(|privacy_usage| privacy_usage.is_some())
        .map(|privacy_usage| privacy_usage.unwrap())
        // linear sum
        .fold1(|usage_1, usage_2| privacy_usage_reducer(
            &usage_1, &usage_2, &|l, r| l + r));

    // TODO: this should probably return a yarrow::PrivacyUsage with zero based on the privacy definition
    match usage_option {
        Some(x) => Ok(x),
        None => Err("no information is released; privacy usage is none")
    }
}

pub fn get_component_privacy_usage(
    component: &yarrow::Component,
    release_node: Option<&yarrow::ReleaseNode>
) -> Option<yarrow::PrivacyUsage> {

    let privacy_usage_option: Option<yarrow::PrivacyUsage> = match component.to_owned().value? {
        yarrow::component::Value::Dpsum(x) => x.privacy_usage,
        yarrow::component::Value::Dpcount(x) => x.privacy_usage,
        yarrow::component::Value::Dpmean(x) => x.privacy_usage,
        yarrow::component::Value::Dpvariance(x) => x.privacy_usage,
        yarrow::component::Value::Dpmomentraw(x) => x.privacy_usage,
        yarrow::component::Value::Dpvariance(x) => x.privacy_usage,
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
    left: &yarrow::PrivacyUsage,
    right: &yarrow::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64
) -> yarrow::PrivacyUsage {

    use yarrow::privacy_usage::Usage as Usage;

    yarrow::PrivacyUsage {
        usage: match (left.usage.to_owned().unwrap(), right.usage.to_owned().unwrap()) {
            (Usage::DistancePure(x), Usage::DistancePure(y)) => Some(Usage::DistancePure(yarrow::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Usage::DistanceApproximate(x), Usage::DistanceApproximate(y)) => Some(Usage::DistanceApproximate(yarrow::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta)
            })),
            _ => None
        }
    }
}

// TODO: implement constraint propagation
pub fn propagate_constraints(
    analysis: &yarrow::Analysis,
    release: &yarrow::Release
) -> Result<HashMap<u32, yarrow::Constraint>, &'static str> {
    Ok(hashmap![])
}

pub fn expand_graph(
    analysis: &yarrow::Analysis,
    release: &yarrow::Release
) -> Result<yarrow::Analysis, &'static str> {
    match propagate_constraints(analysis, release) {
        Ok(constraints) => expand_graph_recursive(analysis, &constraints),
        Err(e) => Err(e)
    }
}

pub fn expand_graph_recursive(
    analysis: &yarrow::Analysis,
    constraints: &HashMap<u32, yarrow::Constraint>
) -> Result<yarrow::Analysis, &'static str> {

    let mut graph: HashMap<u32, yarrow::Component> = analysis.graph.to_owned();
    let mut extension: HashMap<u32, yarrow::Component> = HashMap::new();
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
            &yarrow::Constraint{},
            *node_id,
            max_node_id
        );
        max_node_id = result.0;
        extension.extend(result.1);
    });

    graph.extend(extension);
    Ok(yarrow::Analysis {
        graph: graph,
        privacy_definition: None
    })
}

// TODO: insert component expansions
pub fn expand_component(
    component: &yarrow::Component,
    privacy_definition: &yarrow::PrivacyDefinition,
    constraint: &yarrow::Constraint,
    component_id: u32,
    minimum_id: u32
) -> (u32, HashMap<u32, yarrow::Component>) {
    let mut current_id = minimum_id.clone();
    let arguments = component.arguments.to_owned();

    let mut constraints: HashMap<u32, yarrow::Constraint> = HashMap::new();

    let mut graph: HashMap<u32, yarrow::Component> = HashMap::new();

    match component.value.to_owned().unwrap() {
        yarrow::component::Value::Dpmean(x) => {
            // impute
            current_id += 1;
            let id_impute = current_id.clone();
            graph.insert(id_impute, yarrow::Component {
                arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
                value: Some(yarrow::component::Value::Impute(yarrow::Impute {})),
                omit: true, batch: component.batch
            });
            constraints.insert(id_impute, constraint.to_owned());
            // mean
            current_id += 1;
            let id_mean = current_id.clone();
            graph.insert(id_mean, yarrow::Component {
                arguments: hashmap!["data".to_owned() => id_impute],
                value: Some(yarrow::component::Value::Mean(yarrow::Mean {})),
                omit: true, batch: component.batch
            });
            // noising
            current_id += 1;
            let id_count = current_id.clone();
            graph.insert(id_count, yarrow::Component {
                arguments: hashmap!["data".to_owned() => id_mean],
                value: Some(yarrow::component::Value::LaplaceMechanism(yarrow::LaplaceMechanism {
                    privacy_usage: x.privacy_usage,
                    sensitivity: compute_sensitivity(component, constraint, privacy_definition)
                })),
                omit: true, batch: component.batch
            });

            (current_id, expand_graph_recursive(&yarrow::Analysis {
                graph,
                privacy_definition: None
            }, &constraints).unwrap().graph)
        },
        _ => (minimum_id, hashmap![component_id => component.to_owned()])
    }
}

// TODO: insert sensitivity derivations
pub fn compute_sensitivity(
    component: &yarrow::Component,
    constraint: &yarrow::Constraint,
    privacy_definition: &yarrow::PrivacyDefinition
) -> f64 {
    1.
}

// TODO: create report json
pub fn generate_report(
    analysis: &yarrow::Analysis,
    release: &yarrow::Release
) -> Result<String, &'static str> {
    return Ok("{\"key\": \"value\"}".to_owned());
}

// TODO: insert accuracy to privacy_usage derivations
pub fn accuracy_to_privacy_usage(
    privacy_definition: &yarrow::PrivacyDefinition,
    component: &yarrow::Component,
    constraint: &yarrow::Constraint,
    accuracy: &yarrow::Accuracy
) -> Result<yarrow::PrivacyUsage, &'static str> {
    Ok(yarrow::PrivacyUsage {
        usage: Some(yarrow::privacy_usage::Usage::DistancePure(yarrow::privacy_usage::DistancePure {
            epsilon: 0.
        }))
    })
}

// TODO: insert privacy_usage to accuracy derivations
pub fn privacy_usage_to_accuracy(
    privacy_definition: &yarrow::PrivacyDefinition,
    component: &yarrow::Component,
    constraint: &yarrow::Constraint
) -> Result<yarrow::Accuracy, &'static str> {
    Ok(yarrow::Accuracy {value: 12.})
}
