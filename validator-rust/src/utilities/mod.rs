pub mod json;
pub mod serial;
pub mod inference;
pub mod array;

use crate::errors::*;

use crate::{proto, base};

use crate::base::{Release, Value, ValueProperties, SensitivitySpace, NodeProperties, ReleaseNode};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use crate::utilities::serial::{parse_value_properties, serialize_value, parse_release_node};
use crate::utilities::inference::infer_property;

use itertools::Itertools;
use ndarray::prelude::*;

// import all trait implementations
use crate::components::*;
use crate::utilities::array::slow_select;
use noisy_float::prelude::n64;
use std::iter::FromIterator;
use crate::ffi::serialize_error;

/// Retrieve the Values for each of the arguments of a component from the Release.
pub fn get_public_arguments(
    component: &proto::Component,
    release: &Release,
) -> Result<HashMap<String, Value>> {
    let mut arguments = HashMap::<String, Value>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(evaluation) = release.get(&field) {
            if evaluation.public {
                arguments.insert(field_id.to_owned(), evaluation.to_owned().value.clone());
            }
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
        _ => Err((name.to_string() + " must be defined").into())
    }
}

/// Retrieve the ValueProperties for each of the arguments of a component from the Release.
pub fn get_input_properties<T>(
    component: &proto::Component,
    graph_properties: &HashMap<u32, T>,
) -> Result<HashMap<String, T>> where T: std::clone::Clone {
    let mut properties = HashMap::<String, T>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(property) = graph_properties.get(&field) {
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
    analysis: &mut proto::Analysis,
    release: &mut base::Release,
    properties: Option<HashMap<u32, proto::ValueProperties>>,
    dynamic: bool,
) -> Result<(HashMap<u32, ValueProperties>, Vec<proto::Error>)> {
    let ref mut graph = analysis.computation_graph.as_mut()
        .ok_or_else(|| Error::from("computation_graph must be defined"))?.value;
    let mut traversal: Vec<u32> = get_traversal(&graph)?;

    // extend and pop from the end of the traversal
    traversal.reverse();

    let mut graph_properties = match properties {
        Some(properties) => properties.into_iter()
            .map(|(idx, props)| (idx.clone(), parse_value_properties(props)))
            .collect::<HashMap<u32, ValueProperties>>(),
        None => HashMap::new()
    };

    // infer properties on public evaluations
    graph_properties.extend(release.iter()
        .filter(|(_, release_node)| release_node.public)
        .map(|(node_id, release_node)| Ok((*node_id, infer_property(&release_node.value, Some(*node_id as i64))?)))
        .collect::<Result<HashMap<u32, ValueProperties>>>()?);

    let mut maximum_id = graph.keys().cloned()
        .fold(0, std::cmp::max);

    let mut failed_ids = HashSet::new();

    let mut warnings = Vec::new();

    while !traversal.is_empty() {
        let node_id = *traversal.last().unwrap();

        let component: proto::Component = graph.get(&node_id).unwrap().to_owned();

        if component.arguments.values().any(|v| failed_ids.contains(v)) {
            failed_ids.insert(traversal.pop().unwrap());
            continue;
        }

        let input_properties = get_input_properties(&component, &graph_properties)?;
        let public_arguments = get_public_arguments(&component, &release)?;

        let mut expansion = match (dynamic, component.clone().variant
            .ok_or_else(|| Error::from("component variant must be defined"))?
            .expand_component(
                &analysis.privacy_definition,
                &component,
                &input_properties,
                &node_id,
                &maximum_id,
            )) {
            (_, Ok(expansion)) => expansion,

            (true, Err(err)) => {
                failed_ids.insert(traversal.pop().unwrap());
                warnings.push(serialize_error(err));
                continue;
            }
            (false, Err(err)) => return Err(err)
        };

        // patch the computation graph
        graph.extend(expansion.computation_graph.clone());
        graph_properties.extend(expansion.properties.into_iter()
            .map(|(node_id, props)| (node_id, parse_value_properties(props)))
            .collect::<HashMap<u32, ValueProperties>>());
        release.extend(expansion.releases.into_iter()
            .map(|(node_id, release)| (node_id, parse_release_node(release)))
            .collect::<HashMap<u32, ReleaseNode>>());

        maximum_id = *expansion.computation_graph.keys().max()
            .map(|v| v.max(&maximum_id)).unwrap_or(&maximum_id);

        // if patch added nodes, extend the traversal
        if !expansion.traversal.is_empty() {
            expansion.traversal.reverse();
            traversal.extend(expansion.traversal);
            continue;
        }
        traversal.pop();

        let component_properties = match release.get(&node_id) {
            // if node has already been evaluated, infer properties directly from the public data
            Some(release_node) => {
                if release_node.public {
                    infer_property(&release_node.value, Some(node_id as i64))
                } else {
                    let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
                    component.clone().variant
                        .ok_or_else(|| Error::from("privacy definition must be defined"))?
                        .propagate_property(
                            &analysis.privacy_definition, &public_arguments, &input_properties, node_id)
                        .chain_err(|| format!("at node_id {:?}", node_id))
                }
            }

            // if node has not been evaluated, propagate properties over it
            None => {
                let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
                component.clone().variant.unwrap().propagate_property(
                    &analysis.privacy_definition, &public_arguments, &input_properties, node_id)
                    .chain_err(|| format!("at node_id {:?}", node_id))
            }
        };

        let component_properties = match (dynamic, component_properties) {
            (_, Ok(properties)) => properties,
            (true, Err(err)) => {
                failed_ids.insert(node_id);
                warnings.push(serialize_error(err));
                continue;
            }
            (false, Err(err)) => return Err(err)
        };

//        println!("graph evaluation in prop {:?}", graph_evaluation);
        graph_properties.insert(node_id.clone(), component_properties);
    }
    Ok((graph_properties, warnings))
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
        parents.entry(*node_id)
            .or_insert_with(HashSet::<u32>::new);

        component.arguments.values().for_each(|argument_node_id| {
            parents.entry(*argument_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        });
    });

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments.is_empty()
            || component.arguments.values().all(|arg_idx| !graph.contains_key(arg_idx)))
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
    Ok(traversal)
}

/// Retrieve the set of node ids in a graph that have no dependent nodes.
///
/// # Arguments
/// * `computation_graph` - a prost protobuf hashmap representing a computation graph
///
/// # Returns
/// The set of node ids that have no dependent nodes
pub fn get_sinks(computation_graph: &HashMap<u32, proto::Component>) -> HashSet<u32> {
    // start with all nodes
    let mut node_ids = HashSet::from_iter(computation_graph.keys().cloned());

    // remove nodes that are referenced in arguments
    computation_graph.values()
        .for_each(|component| component.arguments.values()
            .for_each(|source_node_id| {
                node_ids.remove(source_node_id);
            }));

    return node_ids;
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
        1 => if value.len() as i64 == *length {
            Ok(value.clone())
        } else { Err("value is of incompatible length".into()) },
        _ => Err("value must be a scalar or vector".into())
    }
}

#[doc(hidden)]
pub fn uniform_density(length: usize) -> Vec<f64> {
    (0..length).map(|_| 1. / (length as f64)).collect()
}


/// Convert weights to probabilities
#[doc(hidden)]
pub fn normalize_probabilities(weights: &[f64]) -> Result<Vec<f64>> {
    if !weights.iter().all(|w| w >= &0.) {
        return Err("all weights must be greater than zero".into());
    }
    let sum: f64 = weights.iter().sum();
    Ok(weights.iter().map(|prob| prob / sum).collect())
}

pub fn standardize_float_argument(
    categories: &[Vec<f64>],
    length: &i64,
) -> Result<Vec<Vec<f64>>> {
    let mut categories = categories.to_owned();

    if categories.is_empty() {
        return Err("no categories are defined".into());
    }

    categories.clone().into_iter().map(|mut col| {
        if !col.iter().all(|v| v.is_finite()) {
            return Err("all floats must be finite".into());
        }

        col.sort_unstable_by(|l, r| l.partial_cmp(r).unwrap());

        let original_length = col.len();

        if deduplicate(col.into_iter().map(n64).collect()).len() < original_length {
            return Err("floats must not contain duplicates".into());
        }
        Ok(())
    }).collect::<Result<()>>()?;

    // broadcast categories across all columns, if only one categories set is defined
    if categories.len() == 1 {
        categories = (0..*length).map(|_| categories.first().unwrap().clone()).collect();
    }

    Ok(categories)
}

/// Given a jagged categories array, conduct well-formedness checks and broadcast
#[doc(hidden)]
pub fn standardize_categorical_argument<T: Clone + Eq + Hash + Ord>(
    categories: Vec<Vec<T>>,
    length: &i64,
) -> Result<Vec<Vec<T>>> {
    // deduplicate categories
    let mut categories = categories.into_iter()
        .map(deduplicate).collect::<Vec<Vec<T>>>();

    if categories.is_empty() {
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
    value: &Vec<Vec<T>>,
    length: &i64,
) -> Result<Vec<Vec<T>>> {
    let mut value = value.clone();

    if value.is_empty() {
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
    if value.is_empty() {
        return Err("null values cannot be empty".into());
    }

    if value.len() == *length as usize {
        return Ok(value.iter().cloned().collect());
    }

    // broadcast nulls across all columns, if only one null is defined
    if value.len() == 1 {
        let value = value.first().unwrap();
        return Ok((0..*length).map(|_| value.clone()).collect());
    }

    bail!("length of null must be one, or {}", length)
}

/// Given categories and a jagged categories weights array, conduct well-formedness checks and return a standardized set of probabilities.
#[doc(hidden)]
pub fn standardize_weight_argument(
    weights: &Option<Vec<Vec<f64>>>,
    lengths: &[i64],
) -> Result<Vec<Vec<f64>>> {
    let weights = weights.clone().unwrap_or_else(|| vec![]);

    match weights.len() {
        0 => Ok(lengths.iter()
            .map(|length| uniform_density(*length as usize))
            .collect::<Vec<Vec<f64>>>()),
        1 => {
            let probabilities = normalize_probabilities(&weights[0])?;

            lengths.iter()
                .map(|length| if *length as usize == weights.len() {
                    Ok(probabilities.clone())
                } else {
                    Err("length of weights does not match number of categories".into())
                }).collect::<Result<Vec<Vec<f64>>>>()
        }
        _ => if lengths.len() == weights.len() {
            weights.iter().map(|v| normalize_probabilities(v)).collect::<Result<Vec<Vec<f64>>>>()
        } else {
            Err("category weights must be the same length as categories, or none".into())
        }
    }
}

/// Utility for building extra Components to pass back when conducting expansions.
#[doc(hidden)]
pub fn get_literal(value: Value, batch: &u32) -> Result<(proto::Component, proto::ReleaseNode)> {
    Ok((
        proto::Component {
            arguments: HashMap::new(),
            variant: Some(proto::component::Variant::Literal(proto::Literal {})),
            omit: true,
            batch: *batch,
        },
        proto::ReleaseNode {
            value: Some(serialize_value(value)),
            privacy_usages: None,
            public: true,
        }
    ))
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
    release_node.map(|v| if let Some(release_privacy_usage) = v.privacy_usages.clone() {
        privacy_usage = release_privacy_usage.values
    });

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
            (Distance::Pure(x), Distance::Pure(y)) => Some(Distance::Pure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Distance::Approximate(x), Distance::Approximate(y)) => Some(Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta),
            })),
            _ => None
        }
    }
}

pub fn privacy_usage_check(
    privacy : &proto::PrivacyUsage
) -> Result<()> {
    use proto::privacy_usage::Distance as Distance;
    // helper functions that check that privacy parameters lie in reasonable ranges
    let check_epsilon = |privacy_param: f64| -> Result<()> {
        if privacy_param <= 0.0 {
            return Err("Privacy parameter epsilon must be greater than 0.".into())
        } else if privacy_param > 1.0{
            println!("Large value of privacy parameter epsilon in use.");
        }
        Ok(())
    };
    let check_delta = |privacy_param: f64| -> Result<()> {
        if privacy_param < 0.0 {
            return Err("Privacy parameter delta must be non-negative.".into())
        } else if privacy_param > 1.0{
            return Err("Privacy parameter delta must be at most 1.".into())
        }
        Ok(())
    };
    match privacy.distance.as_ref()
        .ok_or_else(|| Error::from("distance must be defined"))? {
        Distance::Pure(x) => {
            check_epsilon(x.epsilon)?;
        },
        Distance::Approximate(x) => {
            check_epsilon(x.epsilon)?;
            check_delta(x.delta)?;
        }
    };
    Ok(())
}

pub fn get_epsilon(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::Pure(distance) => Ok(distance.epsilon),
        proto::privacy_usage::Distance::Approximate(distance) => Ok(distance.epsilon),
//        _ => Err("epsilon is not defined".into())
    }
}

pub fn get_delta(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::Approximate(distance) => Ok(distance.delta),
        _ => Err("delta is not defined".into())
    }
}

pub fn broadcast_privacy_usage(usages: &[proto::PrivacyUsage], length: usize) -> Result<Vec<proto::PrivacyUsage>> {
    if usages.len() == length {
        return Ok(usages.to_owned());
    }

    if usages.len() != 1 {
        bail!("{} privacy parameters passed when {} were required", usages.len(), length);
    }

    Ok(match usages[0].distance.clone().ok_or("distance must be defined on a privacy usage")? {
        proto::privacy_usage::Distance::Pure(pure) => (0..length)
            .map(|_| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Pure(proto::privacy_usage::DistancePure {
                    epsilon: pure.epsilon / (length as f64)
                }))
            }).collect(),
        proto::privacy_usage::Distance::Approximate(approx) => (0..length)
            .map(|_| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: approx.epsilon / (length as f64),
                    delta: approx.delta / (length as f64),
                }))
            }).collect()
    })
}

pub fn broadcast_ndarray<T: Clone>(value: &ArrayD<T>, shape: &[usize]) -> Result<ArrayD<T>> {
    if value.shape() == shape {
        return Ok(value.clone());
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
    privacy_definition: &Option<proto::PrivacyDefinition>,
    component: &proto::Component,
    properties: &NodeProperties,
    component_id: &u32,
    maximum_id: &u32,
) -> Result<proto::ComponentExpansion> {
    let privacy_definition = privacy_definition.as_ref()
        .ok_or_else(|| "privacy definition must be defined")?;
    let mut current_id = *maximum_id;
    let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

    // always overwrite sensitivity. This is not something a user may configure
    let data_property = properties.get("data")
        .ok_or("data: missing")?.array()
        .map_err(prepend("data:"))?.clone();

    let aggregator = data_property.aggregator
        .ok_or_else(|| Error::from("aggregator: missing"))?;

    let sensitivity = aggregator.component.compute_sensitivity(
        privacy_definition,
        &aggregator.properties,
        &sensitivity_type)?;

    current_id += 1;
    let id_sensitivity = current_id;
    let (patch_node, release) = get_literal(sensitivity, &component.batch)?;
    computation_graph.insert(id_sensitivity.clone(), patch_node);
    releases.insert(id_sensitivity.clone(), release);

    // noising
    let mut noise_component = component.clone();
    noise_component.arguments.insert("sensitivity".to_string(), id_sensitivity);

    if sensitivity_type == &SensitivitySpace::Exponential {
        let utility = component
            .variant.as_ref()
            .ok_or_else(|| Error::from("aggregator variant must be defined"))?
            .get_utility(properties)?;

        current_id += 1;
        let id_utility = current_id;
        let (patch_node, release) = get_literal(Value::Function(utility), &component.batch)?;
        computation_graph.insert(id_utility.clone(), patch_node);
        releases.insert(id_utility.clone(), release);
        noise_component.arguments.insert("utility".to_string(), id_utility);
    }

    computation_graph.insert(component_id.clone(), noise_component);

    Ok(proto::ComponentExpansion {
        computation_graph,
        properties: HashMap::new(),
        releases,
        traversal: Vec::new(),
    })
}

pub fn get_ith_column<T: Clone + Default>(value: &ArrayD<T>, i: &usize) -> Result<ArrayD<T>> {
    match value.ndim() {
        0 => if i == &0 { Ok(value.clone()) } else { Err("ith release does not exist".into()) },
        1 => Ok(value.clone()),
        2 => {
            let release = slow_select(value, Axis(1), &[*i]);
            if release.len() == 1 {
                // flatten singleton matrices to zero dimensions
                Ok(Array::from_shape_vec(Vec::new(), vec![release.first()
                    .ok_or_else(|| Error::from("release must contain at least one value"))?])?
                    .mapv(|v| v.clone()))
            } else {
                Ok(release)
            }
        }
        _ => Err("releases must be 2-dimensional or less".into())
    }
}

pub fn deduplicate<T: Eq + Hash + Ord + Clone>(values: Vec<T>) -> Vec<T> {
    values.into_iter().unique().collect()
}


#[cfg(test)]
mod test_utilities {
    use crate::utilities;

    #[test]
    fn test_deduplicate() {
        let values = vec![2, 0, 1, 0];
        let deduplicated = utilities::deduplicate(values.clone());
        assert!(deduplicated == vec![2, 0, 1]);
    }
}