use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter::FromIterator;

use indexmap::map::IndexMap;
use itertools::Itertools;
use ndarray::prelude::*;
use noisy_float::prelude::n64;

use crate::{base, Float, proto, Warnable};
use crate::base::{IndexKey, NodeProperties, Release, SensitivitySpace, Value, ValueProperties, ArrayProperties, Array};
// import all trait implementations
use crate::components::*;
use crate::errors::*;
use crate::utilities::inference::infer_property;
use crate::utilities::privacy::spread_privacy_usage;
use std::ops::MulAssign;

pub mod json;
pub mod inference;
pub mod serial;
pub mod array;
pub mod privacy;
pub mod properties;

/// Retrieve the specified Value from the arguments to a component.
pub fn take_argument(
    arguments: &mut IndexMap<base::IndexKey, Value>,
    name: &str,
) -> Result<Value> {
    arguments.remove::<base::IndexKey>(&name.into())
        .ok_or_else(|| Error::from(name.to_string() + " must be defined"))
}

pub fn get_argument<'a>(
    arguments: &IndexMap<base::IndexKey, &'a Value>,
    name: &str,
) -> Result<&'a Value> {
    arguments.get::<base::IndexKey>(&name.into()).cloned()
        .ok_or_else(|| Error::from(name.to_string() + " must be defined"))
}

/// Retrieve the Values for each of the arguments of a component from the Release.
pub fn get_public_arguments<'a>(
    component: &proto::Component,
    release: &'a Release,
) -> Result<IndexMap<base::IndexKey, &'a Value>> {
    let mut arguments = IndexMap::<base::IndexKey, &'a Value>::new();
    for (arg_name, arg_node_id) in component.arguments() {
        if let Some(evaluation) = release.get(&arg_node_id) {
            if evaluation.public {
                arguments.insert(arg_name.to_owned(), &evaluation.value);
            }
        }
    }
    Ok(arguments)
}

/// Retrieve the ValueProperties for each of the arguments of a component from the Release.
pub fn get_input_properties<T>(
    component: &proto::Component,
    graph_properties: &HashMap<u32, T>,
) -> Result<IndexMap<base::IndexKey, T>> where T: std::clone::Clone {
    let mut properties = IndexMap::<base::IndexKey, T>::new();
    for (arg_name, arg_node_id) in component.arguments() {
        if let Some(property) = graph_properties.get(&arg_node_id) {
            properties.insert(arg_name.to_owned(), property.clone());
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
    privacy_definition: &Option<proto::PrivacyDefinition>,
    computation_graph: &mut HashMap<u32, proto::Component>,
    release: &mut base::Release,
    properties: Option<HashMap<u32, base::ValueProperties>>,
    dynamic: bool
) -> Result<(HashMap<u32, ValueProperties>, Vec<Error>)> {
    let mut traversal: Vec<u32> = get_traversal(&computation_graph)?;
    // extend and pop from the end of the traversal
    traversal.reverse();

    let mut properties = properties.unwrap_or_else(HashMap::new);

    let mut maximum_id = computation_graph.keys().max().cloned().unwrap_or(0);
    // println!("maximum node id: {:?}", maximum_id);
    // let maximum_property_id = graph_properties.keys().max().cloned().unwrap_or(0);
    // println!("maximum property id: {:?}", maximum_property_id);
    // let maximum_release_id = release.keys().max().cloned().unwrap_or(0);
    // println!("maximum release id: {:?}", maximum_release_id);

    // infer properties on public evaluations
    properties.extend(release.iter()
        .filter(|(_, release_node)| release_node.public)
        .map(|(node_id, release_node)|
            Ok((*node_id, infer_property(
                &release_node.value,
                properties.get(node_id), *node_id)?)))
        .collect::<Result<HashMap<u32, ValueProperties>>>()?);


    let mut failed_ids = HashSet::new();

    let mut warnings = Vec::new();

    while !traversal.is_empty() {
        let node_id = *traversal.last().unwrap();

        let component: &proto::Component = computation_graph.get(&node_id).unwrap();

        // println!("component {:?}", component);

        if component.arguments().values().any(|v| failed_ids.contains(v)) {
            failed_ids.insert(traversal.pop().unwrap());
            continue;
        }

        let mut expansion = match component
            .expand_component(
                privacy_definition,
                &component,
                &get_public_arguments(component, &release)?,
                &get_input_properties(&component, &properties)?,
                node_id,
                maximum_id,
            ) {
            Ok(expansion) => expansion,
            Err(err) => if dynamic {
                failed_ids.insert(traversal.pop().unwrap());
                warnings.push(err);
                continue;
            } else { return Err(err) }
        };

        maximum_id = expansion.computation_graph.keys().max().cloned()
            .unwrap_or(0).max(maximum_id);

        // patch the computation graph
        computation_graph.extend(expansion.computation_graph);
        properties.extend(expansion.properties);
        release.extend(expansion.releases);
        warnings.extend(expansion.warnings);

        // if patch added nodes, extend the traversal
        if !expansion.traversal.is_empty() {
            expansion.traversal.reverse();
            traversal.extend(expansion.traversal);
            continue;
        }

        //component may have changed since the last call, due to the expansion
        let component: &proto::Component = computation_graph.get(&node_id).unwrap();

        let mut input_properties = IndexMap::<base::IndexKey, ValueProperties>::new();
        let mut missing_properties = Vec::new();
        for (arg_name, arg_node_id) in component.arguments() {
            if let Some(property) = properties.get(&arg_node_id) {
                input_properties.insert(arg_name.to_owned(), property.clone());
            } else {
                missing_properties.push(arg_node_id);
            }
        }
        if !missing_properties.is_empty() {
            traversal.extend(missing_properties);
            continue
        }

        traversal.pop();

        let release_node = release.get(&node_id);
        // println!("release node {:?}", release_node);

        let propagation_result = if release_node
            .map(|release_node| release_node.public).unwrap_or(false) {
            // if node has already been evaluated and is public, infer properties directly from the public data
            // println!("inferring property");
            Ok(Warnable(infer_property(
                &release_node.unwrap().value,
                properties.get(&node_id), node_id)?, vec![]))
        } else {
            // if node has not been evaluated, propagate properties over it
            computation_graph.get(&node_id).unwrap()
                .propagate_property(
                    privacy_definition,
                    get_public_arguments(component, &release)?,
                    input_properties,
                    node_id)
                .chain_err(|| format!("at node_id {:?}", node_id))
        };

        // println!("prop result: {:?}", propagation_result);
        match propagation_result {
            Ok(propagation_result) => {
                let Warnable(component_properties, propagation_warnings) = propagation_result;

                warnings.extend(propagation_warnings.into_iter()
                    .map(|err| err.chain_err(|| format!("at node_id {:?}", node_id)))
                    .collect::<Vec<Error>>());

                properties.insert(node_id, component_properties);
            },
            Err(err) => if dynamic {
                failed_ids.insert(node_id);
                warnings.push(err);
            } else { return Err(err) }
        };
    }
    // println!("done propagating");
    Ok((properties, warnings))
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

        component.arguments().values().for_each(|argument_node_id| {
            parents.entry(*argument_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        });
    });

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments().is_empty()
            || component.arguments().values().all(|arg_idx| !graph.contains_key(arg_idx)))
        .map(|(node_id, _component)| node_id.to_owned()).collect();

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        let mut is_cyclic = false;

        parents.get(&queue_node_id).unwrap().iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id).unwrap().to_owned().arguments();

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
        .for_each(|component| component.arguments().values()
            .for_each(|source_node_id| {
                node_ids.remove(source_node_id);
            }));

    node_ids
}


/// Sets the node id of properties
///
pub fn set_node_id(property: &mut ValueProperties, node_id: u32) -> () {
    match property {
        ValueProperties::Array(array) => array.node_id = node_id as i64,
        ValueProperties::Dataframe(dataframe) => dataframe.children.iter_mut()
            .for_each(|(_k, v)| set_node_id(v, node_id)),
        ValueProperties::Partitions(partitions) => partitions.children.iter_mut()
            .for_each(|(_k, v)| set_node_id(v, node_id)),
        ValueProperties::Jagged(_) => (),
        ValueProperties::Function(_) => ()
    };
}

/// Given an array, conduct well-formedness checks and broadcast
///
/// Typically used by functions when standardizing numeric arguments, but generally applicable.
#[doc(hidden)]
pub fn standardize_numeric_argument<T: Clone>(value: ArrayD<T>, length: i64) -> Result<ArrayD<T>> {
    match value.ndim() {
        0 => match value.first() {
            Some(scalar) => Ok(ndarray::Array::from((0..length).map(|_| scalar.clone())
                .collect::<Vec<T>>()).into_dyn()),
            None => Err("value must be non-empty".into())
        },
        1 => if value.len() as i64 == length {
            Ok(value)
        } else { Err("value is of incompatible length".into()) },
        _ => Err("value must be a scalar or vector".into())
    }
}

/// Given a jagged float array, conduct well-formedness checks and broadcast
pub fn standardize_float_argument(
    mut categories: Vec<Vec<Float>>,
    length: i64,
) -> Result<Vec<Vec<Float>>> {

    if categories.is_empty() {
        return Err("no categories are defined".into());
    }

    categories.clone().into_iter().try_for_each(|mut col| {
        if !col.iter().all(|v| v.is_finite()) {
            return Err("all floats must be finite".into());
        }

        col.sort_unstable_by(|l, r| l.partial_cmp(r).unwrap());

        let original_length = col.len();

        // TODO cfg conditional compilation to n32
        if deduplicate(col.into_iter()
            .map(|v| n64(v as f64)).collect()).len() < original_length {
            return Err("floats must not contain duplicates".into());
        }
        Ok::<_, Error>(())
    })?;

    // broadcast categories across all columns, if only one categories set is defined
    if categories.len() == 1 {
        categories = (0..length).map(|_| categories.first().unwrap().clone()).collect();
    }

    Ok(categories)
}

/// Given a jagged categories array, conduct well-formedness checks and broadcast
#[doc(hidden)]
pub fn standardize_categorical_argument<T: Clone + Eq + Hash + Ord>(
    categories: Vec<Vec<T>>,
    length: i64,
) -> Result<Vec<Vec<T>>> {
    // deduplicate categories
    let mut categories = categories.into_iter()
        .map(deduplicate).collect::<Vec<Vec<T>>>();

    if categories.is_empty() {
        return Err("no categories are defined".into());
    }
    // broadcast categories across all columns, if only one categories set is defined
    if categories.len() == 1 {
        categories = (0..length).map(|_| categories.first().unwrap().clone()).collect();
    }

    Ok(categories)
}


/// Given a jagged null values array,
///    conduct well-formedness checks, broadcast along columns, and flatten along rows.
#[doc(hidden)]
pub fn standardize_null_candidates_argument<T: Clone>(
    mut value: Vec<Vec<T>>,
    length: i64,
) -> Result<Vec<Vec<T>>> {

    if value.is_empty() {
        return Err("null values cannot be an empty vector".into());
    }

    // broadcast nulls across all columns, if only one null set is defined
    if value.len() == 1 {
        let first_set = value.first().unwrap();
        value = (0..length).map(|_| first_set.clone()).collect();
    }
    Ok(value)
}

/// Given a jagged null values array,
///    conduct well-formedness checks, broadcast along columns, and flatten along rows.
#[doc(hidden)]
pub fn standardize_null_target_argument<T: Clone>(
    value: ArrayD<T>,
    length: i64,
) -> Result<Vec<T>> {
    if value.is_empty() {
        return Err("null values cannot be empty".into());
    }

    if value.len() == length as usize {
        return Ok(value.iter().cloned().collect());
    }

    // broadcast nulls across all columns, if only one null is defined
    if value.len() == 1 {
        let value = value.first().unwrap();
        return Ok((0..length).map(|_| value.clone()).collect());
    }

    bail!("length of null must be one, or {}", length)
}

/// Given categories and a jagged categories weights array,
///    conduct well-formedness checks and return a standardized set of probabilities.
#[doc(hidden)]
pub fn standardize_weight_argument(
    weights: &Option<Vec<Vec<Float>>>,
    lengths: &[i64],
) -> Result<Vec<Vec<Float>>> {
    let weights = weights.clone().unwrap_or_else(Vec::new);

    fn uniform_density(length: usize) -> Vec<Float> {
        (0..length).map(|_| 1. / (length as Float)).collect()
    }
    /// Convert weights to probabilities
    fn normalize_probabilities(weights: &[Float]) -> Result<Vec<Float>> {
        if !weights.iter().all(|w| w >= &0.) {
            return Err("all weights must be greater than zero".into());
        }
        let sum: Float = weights.iter().sum();
        Ok(weights.iter().map(|prob| prob / sum).collect())
    }

    match weights.len() {
        0 => Ok(lengths.iter()
            .map(|length| uniform_density(*length as usize))
            .collect::<Vec<Vec<Float>>>()),
        1 => {
            let probabilities = normalize_probabilities(&weights[0])?;

            lengths.iter()
                .map(|length| if *length as usize == weights.len() {
                    Ok(probabilities.clone())
                } else {
                    Err("length of weights does not match number of categories".into())
                }).collect::<Result<Vec<Vec<Float>>>>()
        }
        _ => if lengths.len() == weights.len() {
            weights.iter().map(|v| normalize_probabilities(v))
                .collect::<Result<Vec<Vec<Float>>>>()
        } else {
            Err("category weights must be the same length as categories, or none".into())
        }
    }
}

/// Utility for building extra Components to pass back when conducting expansions.
#[doc(hidden)]
pub fn get_literal(value: Value, submission: u32) -> Result<(proto::Component, base::ReleaseNode)> {
    Ok((
        proto::Component {
            arguments: None,
            variant: Some(proto::component::Variant::Literal(proto::Literal {})),
            omit: true,
            submission,
        },
        base::ReleaseNode {
            value,
            privacy_usages: None,
            public: true,
        }
    ))
}

/// return a simple function that modifies the input string with the specified text
/// part of a commonly used pattern to prepend the argument name to an error string
#[doc(hidden)]
pub fn prepend(text: &str) -> impl Fn(Error) -> Error + '_ {
    move |e| format!("{} {}", text, e).into()
}

/// Utility function for building component expansions for dp mechanisms
#[allow(clippy::float_cmp)]
pub fn expand_mechanism(
    sensitivity_type: &SensitivitySpace,
    privacy_definition: &Option<proto::PrivacyDefinition>,
    privacy_usage: &[proto::PrivacyUsage],
    component: &proto::Component,
    properties: &NodeProperties,
    component_id: u32,
    mut maximum_id: u32,
) -> Result<base::ComponentExpansion> {

    let mut expansion = base::ComponentExpansion::default();

    let privacy_definition = privacy_definition.as_ref()
        .ok_or_else(|| "privacy definition must be defined")?;

    // always overwrite sensitivity. This is not something a user may configure
    let data_property: ArrayProperties = properties.get::<IndexKey>(&"data".into())
        .ok_or("data: missing")?.array()
        .map_err(prepend("data:"))?.clone();

    // spread privacy usage over each column
    let spread_usages = spread_privacy_usage(
        // spread usage over each column
        privacy_usage, data_property.num_columns()? as usize)?;

    // convert to effective usage
    let effective_usages = spread_usages.into_iter()
        // reduce epsilon allowed to algorithm based on c-stability and group size
        .map(|usage| usage.actual_to_effective(
            data_property.sample_proportion.unwrap_or(1.),
            data_property.c_stability,
            privacy_definition.group_size))
        .collect::<Result<Vec<proto::PrivacyUsage>>>()?;

    // insert sensitivity and usage
    let mut noise_component = component.clone();

    macro_rules! assign_usage {
        ($($variant:ident),*) => {
            match noise_component.variant.as_mut() {
                $(Some(proto::component::Variant::$variant(variant)) =>
                    variant.privacy_usage = effective_usages,)*
                _ => return Err(Error::from("unrecognized component in expand_mechanism"))
            }
        }
    }
    assign_usage!(LaplaceMechanism, GaussianMechanism, SimpleGeometricMechanism, SnappingMechanism);


    if privacy_definition.protect_sensitivity || !properties.contains_key(&IndexKey::from("sensitivity")) {
        let aggregator = data_property.aggregator.as_ref()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity scaling
        let mut sensitivity_value = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &sensitivity_type)?;

        match aggregator.lipschitz_constants.clone().array()? {
            Array::Float(lipschitz) => {
                if lipschitz.iter().any(|v| v != &1.) {
                    let mut sensitivity = sensitivity_value.array()?.float()?;
                    sensitivity.mul_assign(&lipschitz);
                    sensitivity_value = sensitivity.into();
                }
            },
            Array::Int(lipschitz) => {
                if lipschitz.iter().any(|v| v != &1) {
                    let mut sensitivity = sensitivity_value.array()?.int()?;
                    sensitivity.mul_assign(&lipschitz);
                    sensitivity_value = sensitivity.into();
                }
            },
            _ => return Err(Error::from("lipschitz constants must be numeric"))
        };

        maximum_id += 1;
        let id_sensitivity = maximum_id;
        let (patch_node, release) = get_literal(sensitivity_value.clone(), component.submission)?;
        expansion.computation_graph.insert(id_sensitivity, patch_node);
        expansion.properties.insert(id_sensitivity, infer_property(&release.value, None, id_sensitivity)?);
        expansion.releases.insert(id_sensitivity, release);
        noise_component.insert_argument(&"sensitivity".into(), id_sensitivity);
    }

    expansion.computation_graph.insert(component_id, noise_component);

    Ok(expansion)
}

/// given a vector of items, return the shared item, or None, if no item is shared
#[allow(clippy::ptr_arg)]
pub fn get_common_value<T: Clone + Eq>(values: &Vec<T>) -> Option<T> {
    if values.windows(2).all(|w| w[0] == w[1]) {
        values.first().cloned()
    } else { None }
}

/// return the set of node ids that use each node id
pub fn get_dependents(graph: &HashMap<u32, proto::Component>) -> HashMap<u32, HashSet<u32>> {
    let mut dependents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments().values().for_each(|source_node_id| {
            dependents
                .entry(*source_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });
    dependents
}


pub fn deduplicate<T: Eq + Hash + Clone>(values: Vec<T>) -> Vec<T> {
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