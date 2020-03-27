pub mod json;
pub mod serial;
pub mod inference;
pub mod array;

use crate::errors::*;

use crate::proto;

use crate::base::{Release, Value, ValueProperties, SensitivitySpace, NodeProperties};
use std::collections::{HashMap, HashSet, BTreeSet};
use std::hash::Hash;
use crate::utilities::serial::{parse_release, parse_value_properties, serialize_value, parse_value};
use crate::utilities::inference::infer_property;

use itertools::Itertools;
use ndarray::prelude::*;

// import all trait implementations
use crate::components::*;
use crate::utilities::array::slow_select;
use std::iter::FromIterator;

/// Retrieve the Values for each of the arguments of a component from the Release.
pub fn get_input_arguments(
    component: &proto::Component,
    graph_evaluation: &Release,
) -> Result<HashMap<String, Value>> {
    let mut arguments = HashMap::<String, Value>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(evaluation) = graph_evaluation.get(&field) {
            arguments.insert(field_id.to_owned(), evaluation.to_owned());
        }
    }
    Ok(arguments)
}

/// Retrieve the specified Value from the arguments to a component.
pub fn get_argument<'a>(
    arguments: &HashMap<String, &'a Value>,
    name: &str,
) -> Result<&'a Value> {
    match arguments.get(name) {
        Some(argument) => Ok(argument),
        _ => Err((name.to_string() + " is not defined").into())
    }
}

/// Retrieve the ValueProperties for each of the arguments of a component from the Release.
pub fn get_input_properties<T>(
    component: &proto::Component,
    graph_properties: &HashMap<u32, T>,
) -> Result<HashMap<String, T>> where T: std::clone::Clone {
    let mut properties = HashMap::<String, T>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(property) = graph_properties.get(&field).clone() {
            properties.insert(field_id.to_owned(), property.clone());
        }
    }
    Ok(properties)
}

/// Given an analysis and release, attempt to propagate properties across the entire computation graph.
///
/// The graph is traversed, and every node is attempted to be expanded, so that validation occurs at the most granular level.
/// Each component in the graph implements the Component trait, which contains the propagate_properties function.
/// While traversing, properties are checked and propagated forward at every point in the graph.
/// If the requirements for any node are not met, the propagation fails, and the analysis is not valid.
///
/// # Returns
/// * `0` - Properties for every node in the expanded graph
/// * `1` - The expanded graph
pub fn propagate_properties(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<(HashMap<u32, ValueProperties>, HashMap<u32, proto::Component>)> {
    // compute properties for every node in the graph

    let privacy_definition = analysis.privacy_definition.to_owned().unwrap();
    let mut graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let mut traversal: Vec<u32> = get_traversal(&graph)?;
    // extend and pop from the end of the traversal
    traversal.reverse();

    let mut graph_evaluation: Release = parse_release(&release)?;

    let mut graph_properties = graph_evaluation.iter()
        .map(|(node_id, value)| Ok((node_id.clone(), infer_property(value)?)))
        .collect::<Result<HashMap<u32, ValueProperties>>>()?;

    let mut maximum_id = graph.keys().cloned()
        .fold(0, std::cmp::max);

    while !traversal.is_empty() {
        let node_id = traversal.last().unwrap().clone();

        let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
        let input_properties = get_input_properties(&component, &graph_properties)?;
        let public_arguments = get_input_arguments(&component, &graph_evaluation)?;

        let mut expansion = component.clone().variant.unwrap().expand_component(
            &privacy_definition,
            &component,
            &input_properties,
            &node_id,
            &maximum_id,
        )?;

        // patch the computation graph
        graph.extend(expansion.computation_graph.clone());
        graph_properties.extend(expansion.properties.iter()
            .map(|(node_id, props)| (node_id.clone(), parse_value_properties(props)))
            .collect::<HashMap<u32, ValueProperties>>());
        graph_evaluation.extend(expansion.releases.iter()
            .map(|(node_id, release)| Ok((node_id.clone(), parse_value(&release.value.clone().unwrap())?)))
            .collect::<Result<HashMap<u32, Value>>>()?);

        maximum_id = *expansion.computation_graph.keys().max()
            .map(|v| v.max(&maximum_id)).unwrap_or(&maximum_id);

        // if patch added nodes, extend the traversal
        if !expansion.traversal.is_empty() {
            expansion.traversal.reverse();
            traversal.extend(expansion.traversal);
            continue;
        }
        traversal.pop();

        graph_properties.insert(node_id.clone(), match graph_evaluation.get(&node_id) {
            // if node has already been evaluated, infer properties directly from the public data
            Some(value) => infer_property(&value),

            // if node has not been evaluated, propagate properties over it
            None => {
                let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
                component.clone().variant.unwrap().propagate_property(
                    &privacy_definition, &public_arguments, &input_properties)
                    .chain_err(|| format!("at node_id {:?}", node_id))
            }
        }?);
    }
    Ok((graph_properties, graph))
}

/// Given a computation graph, return an ordering of nodes that ensures all dependencies of any node have been visited
///
/// The traversal also fails upon detecting cyclic dependencies,
/// and attempts to optimize traversal order to minimize caching of intermediate results.
pub fn get_traversal(
    graph: &HashMap<u32, proto::Component>
) -> Result<Vec<u32>> {

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        if !parents.contains_key(node_id) {
            parents.insert(*node_id, HashSet::<u32>::new());
        }
        component.arguments.values().for_each(|argument_node_id| {
            parents.entry(*argument_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments.is_empty())
        .map(|(node_id, _component)| node_id.to_owned()).collect();

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        let mut is_cyclic = false;

        parents.get(&queue_node_id).unwrap().iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id).unwrap().to_owned().arguments;

            // if parent has been reached more times than it has arguments, then it is cyclic
            let count = visited.entry(*parent_node_id).or_insert(0);
            *count += 1;
            if visited.get(parent_node_id).unwrap() > &parent_arguments.len() {
                is_cyclic = true;
            }

            // check that all arguments of parent_node have been evaluated before adding to queue
            if parent_arguments.values().all(|argument_node_id| traversal.contains(argument_node_id)) {
                queue.push(*parent_node_id);
            }
        });

        if is_cyclic {
            return Err("Graph is cyclic.".into());
        }
    }
    return Ok(traversal);
}

/// Given an array, conduct well-formedness checks and broadcast
///
/// Typically used by functions when standardizing numeric arguments, but generally applicable.
#[doc(hidden)]
pub fn standardize_numeric_argument<T: Clone>(value: &ArrayD<T>, length: &i64) -> Result<ArrayD<T>> {
    match value.ndim() {
        0 => match value.first() {
            Some(scalar) => Ok(Array::from((0..*length).map(|_| scalar.clone()).collect::<Vec<T>>()).into_dyn()),
            None => Err("value must be non-empty".into())
        },
        1 => match value.len() as i64 == *length {
            true => Ok(value.clone()),
            false => Err("value is of incompatible length".into())
        },
        _ => Err("value must be a scalar or vector".into())
    }
}

#[doc(hidden)]
pub fn uniform_density(length: usize) -> Vec<f64> {
    (0..length).map(|_| 1. / (length as f64)).collect()
}


/// Convert weights to probabilities
#[doc(hidden)]
pub fn normalize_probabilities(weights: &Vec<f64>) -> Vec<f64> {
    let sum: f64 = weights.iter().sum();
    weights.iter().map(|prob| prob / sum).collect()
}

/// Given a jagged categories array, conduct well-formedness checks and broadcast
#[doc(hidden)]
pub fn standardize_categorical_argument<T: Clone>(
    categories: &Vec<Option<Vec<T>>>,
    length: &i64,
) -> Result<Vec<Vec<T>>> {
    // check that no categories are explicitly None
    let mut categories = categories.iter()
        .map(|v| v.clone())
        .collect::<Option<Vec<Vec<T>>>>()
        .ok_or::<Error>("categories must be defined for all columns".into())?;

    if categories.len() == 0 {
        return Err("no categories are defined".into());
    }
    // broadcast categories across all columns, if only one categories set is defined
    if categories.len() == 1 {
        categories = (0..*length).map(|_| categories.first().unwrap().clone()).collect();
    }

    Ok(categories)
}


/// Given a jagged null values array, conduct well-formedness checks, broadcast along columns, and flatten along rows.
#[doc(hidden)]
pub fn standardize_null_candidates_argument<T: Clone>(
    value: &Vec<Option<Vec<T>>>,
    length: &i64,
) -> Result<Vec<Vec<T>>> {
    let mut value = value.iter()
        .map(|v| v.clone())
        .collect::<Option<Vec<Vec<T>>>>()
        .ok_or::<Error>("null must be defined for all columns".into())?;

    if value.len() == 0 {
        return Err("null values cannot be an empty vector".into());
    }

    // broadcast nulls across all columns, if only one null set is defined
    if value.len() == 1 {
        let first_set = value.first().unwrap();
        value = (0..*length).map(|_| first_set.clone()).collect();
    }
    Ok(value)
}

/// Given a jagged null values array, conduct well-formedness checks, broadcast along columns, and flatten along rows.
#[doc(hidden)]
pub fn standardize_null_target_argument<T: Clone>(
    value: &ArrayD<T>,
    length: &i64,
) -> Result<Vec<T>> {
    if value.len() == 0 {
        return Err("null values cannot be empty".into());
    }

    if value.len() == *length as usize {
        return Ok(value.iter().cloned().collect())
    }

    // broadcast nulls across all columns, if only one null is defined
    if value.len() == 1 {
        let value = value.first().unwrap();
        return Ok((0..*length).map(|_| value.clone()).collect())
    }

    bail!("length of null must be one, or {}", length)
}

/// Given categories and a jagged categories weights array, conduct well-formedness checks and return a standardized set of probabilities.
#[doc(hidden)]
pub fn standardize_weight_argument<T>(
    categories: &Vec<Vec<T>>,
    weights: &Vec<Option<Vec<f64>>>,
) -> Result<Vec<Vec<f64>>> {
    match weights.len() {
        0 => Ok(categories.iter()
            .map(|cats| uniform_density(cats.len()))
            .collect::<Vec<Vec<f64>>>()),
        1 => {
            let weights = match weights[0].clone() {
                Some(weights) => normalize_probabilities(&weights),
                None => uniform_density(categories[0].len())
            };

            categories.iter().map(|cats| match cats.len() == weights.len() {
                true => Ok(weights.clone()),
                false => Err("length of weights does not match number of categories".into())
            }).collect::<Result<Vec<Vec<f64>>>>()
        }
        _ => match categories.len() == weights.len() {
            true => categories.iter().zip(weights.iter()).map(|(_cats, weights)| match weights {
                Some(weights) => Ok(normalize_probabilities(weights)),
                None => Err("category weights must be set once, for all categories, or none".into())
            }).collect::<Result<Vec<Vec<f64>>>>(),
            false => return Err("category weights must be the same length as categories, or none".into())
        }
    }
}

/// Utility for building extra Components to pass back when conducting expansions.
#[doc(hidden)]
pub fn get_literal(value: &Value, batch: &u32) -> Result<(proto::Component, proto::ReleaseNode)> {
    Ok((proto::Component {
        arguments: HashMap::new(),
        variant: Some(proto::component::Variant::Literal(proto::Literal {
            private: false
        })),
        omit: true,
        batch: batch.clone(),
    },
        proto::ReleaseNode {
            value: Some(serialize_value(value)?),
            privacy_usage: Vec::new(),
        }))
}


pub fn get_component_privacy_usage(
    component: &proto::Component,
    release_node: Option<&proto::ReleaseNode>,
) -> Option<proto::PrivacyUsage> {

    // get the maximum possible usage allowed to the component
    let mut privacy_usage: Vec<proto::PrivacyUsage> = match component.to_owned().variant? {
        proto::component::Variant::LaplaceMechanism(x) => x.privacy_usage,
        proto::component::Variant::GaussianMechanism(x) => x.privacy_usage,
        proto::component::Variant::ExponentialMechanism(x) => x.privacy_usage,
        proto::component::Variant::SimpleGeometricMechanism(x) => x.privacy_usage,
        _ => return None
    };

    // if release usage is defined, then use the actual eps, etc. from the release
    if let Some(release_node) = release_node {
        let release_privacy_usage = (*release_node.privacy_usage).to_vec();
        if release_privacy_usage.len() > 0 {
            privacy_usage = release_privacy_usage
        }
    }

    // sum privacy usage within the node
    privacy_usage.into_iter()
        .fold1(|usage_a, usage_b|
            privacy_usage_reducer(&usage_a, &usage_b, &|a, b| a + b))
}

pub fn privacy_usage_reducer(
    left: &proto::PrivacyUsage,
    right: &proto::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64,
) -> proto::PrivacyUsage {
    use proto::privacy_usage::Distance as Distance;

    proto::PrivacyUsage {
        distance: match (left.distance.to_owned().unwrap(), right.distance.to_owned().unwrap()) {
            (Distance::DistancePure(x), Distance::DistancePure(y)) => Some(Distance::DistancePure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Distance::DistanceApproximate(x), Distance::DistanceApproximate(y)) => Some(Distance::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta),
            })),
            _ => None
        }
    }
}

pub fn broadcast_privacy_usage(usages: &Vec<proto::PrivacyUsage>, length: usize) -> Result<Vec<proto::PrivacyUsage>> {
    if usages.len() == length {
        return Ok(usages.clone());
    }

    if usages.len() != 1 {
        bail!("{} privacy parameters passed when {} were required", usages.len(), length);
    }

    Ok(match usages[0].distance.clone().ok_or("distance must be defined on a privacy usage")? {
        proto::privacy_usage::Distance::DistancePure(pure) => (0..length)
            .map(|_| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::DistancePure(proto::privacy_usage::DistancePure {
                    epsilon: pure.epsilon / (length as f64)
                }))
            }).collect(),
        proto::privacy_usage::Distance::DistanceApproximate(approx) => (0..length)
            .map(|_| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: approx.epsilon / (length as f64),
                    delta: approx.delta / (length as f64),
                }))
            }).collect()
    })
}

pub fn broadcast_ndarray<T: Clone>(value: &ArrayD<T>, shape: &[usize]) -> Result<ArrayD<T>> {
    if value.shape() == shape {
        return Ok(value.clone())
    }

    if value.len() != 1 {
        let length = shape.iter().cloned().fold1(|a, b| a * b).unwrap_or(0);
        bail!("{} values passed when {} were required", value.len(), length);
    }

    let value = value.first().unwrap();

    Ok(Array::from_shape_fn(shape, |_| value.clone()))
}

#[doc(hidden)]
pub fn prepend(text: &str) -> impl Fn(Error) -> Error + '_ {
    move |e| format!("{} {}", text, e).into()
}


/// Utility function for building component expansions for dp mechanisms
pub fn expand_mechanism(
    sensitivity_type: &SensitivitySpace,
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    properties: &NodeProperties,
    component_id: &u32,
    maximum_id: &u32,
) -> Result<proto::ComponentExpansion> {
    let mut current_id = maximum_id.clone();
    let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

    // always overwrite sensitivity. This is not something a user may configure
    let data_property = properties.get("data")
        .ok_or("data: missing")?.array()
        .map_err(prepend("data:"))?.clone();

    let aggregator = data_property.aggregator.clone()
        .ok_or::<Error>("aggregator: missing".into())?;

    let sensitivity = aggregator.component.compute_sensitivity(
        privacy_definition,
        &aggregator.properties,
        &sensitivity_type)?;

    current_id += 1;
    let id_sensitivity = current_id.clone();
    let (patch_node, release) = get_literal(&sensitivity, &component.batch)?;
    computation_graph.insert(id_sensitivity.clone(), patch_node);
    releases.insert(id_sensitivity.clone(), release);

    // noising
    let mut noise_component = component.clone();
    noise_component.arguments.insert("sensitivity".to_string(), id_sensitivity);
    computation_graph.insert(component_id.clone(), noise_component);

    Ok(proto::ComponentExpansion {
        computation_graph,
        properties: HashMap::new(),
        releases,
        traversal: Vec::new()
    })
}

pub fn get_ith_release<T: Clone + Default>(value: &ArrayD<T>, i: &usize) -> Result<ArrayD<T>> {
    match value.ndim() {
        0 => if i == &0 {Ok(value.clone())} else {Err("ith release does not exist".into())},
        1 => Ok(value.clone()),
        2 => {
            let release = slow_select(value, Axis(1), &[i.clone()]);
            if release.len() == 1 {
                // flatten singleton matrices to zero dimensions
                Ok(Array::from_shape_vec(Vec::new(), vec![release.first()
                    .ok_or::<Error>("release must contain at least one value".into())?])?
                    .mapv(|v| v.clone()))
            } else {
                Ok(release)
            }
        },
        _ => Err("releases must be 2-dimensional or less".into())
    }
}

pub fn deduplicate<T: Eq + Hash + Ord>(values: Vec<T>) -> Vec<T> {
    BTreeSet::from_iter(values.into_iter()).into_iter().collect()
}