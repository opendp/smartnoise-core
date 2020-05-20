use std::collections::{HashMap, HashSet};
use crate::errors::*;
use itertools::Itertools;
use std::cmp::Ordering;
use crate::proto;
use crate::utilities::serial::serialize_error;

fn get_parents(graph: &HashMap<u32, proto::Component>) -> HashMap<u32, HashSet<u32>> {
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            parents
                .entry(*source_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });
    parents
}

fn compute_branch_privacy_usage(
    node_id: u32,
    graph: HashMap<u32, proto::Component>
) -> Result<proto::PrivacyUsage> {
    Err("test".into())
}

fn compute_graph_privacy_usage(
    graph: &HashMap<u32, proto::Component>
) -> Result<proto::PrivacyUsage> {

    let parents = get_parents(graph);

    Err("test".into())
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
            privacy_usage_reducer(&usage_a, &usage_b, |a, b| a + b))
}


pub fn privacy_usage_reducer(
    left: &proto::PrivacyUsage,
    right: &proto::PrivacyUsage,
    operator: fn(f64, f64) -> f64,
) -> proto::PrivacyUsage {
    use proto::privacy_usage::Distance as Distance;

    proto::PrivacyUsage {
        distance: match (left.distance.to_owned().unwrap(), right.distance.to_owned().unwrap()) {
            (Distance::Approximate(x), Distance::Approximate(y)) => Some(Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta),
            }))
        }
    }
}


pub fn privacy_usage_check(
    privacy_usage: &proto::PrivacyUsage,
    num_records: Option<i64>,
    strict_delta_check: bool,
) -> Result<Vec<Error>> {
    let mut warnings = Vec::new();

    match privacy_usage.distance
        .as_ref().ok_or_else(|| "usage distance must be defined")? {
        proto::privacy_usage::Distance::Approximate(usage) => {
            if usage.epsilon <= 0.0 {
                return Err("epsilon: privacy parameter epsilon must be greater than 0".into());
            }

            if usage.epsilon > 1.0 {
                warnings.push(format!("Warning: A large privacy parameter of epsilon = {} is in use", usage.epsilon.to_string()).into())
            }

            match usage.delta.partial_cmp(&0.0)
                .ok_or_else(|| Error::from("delta: must not be null"))? {
                Ordering::Less => return Err("delta: privacy parameter may not be less than 0".into()),
                Ordering::Equal => (),
                Ordering::Greater => {
                    if usage.delta >= 1.0 {
                        return Err("delta: must be smaller than one".into())
                    }
                    match num_records {
                        Some(num_records) => {
                            if usage.delta * num_records as f64 > 1.0 {
                                return Err("delta: a value greater than 1 / num_records is not differentially private".into())
                            }

                            if usage.delta * num_records.pow(2) as f64 > 1.0 {
                                warnings.push("delta: a value greater than 1 / num_records^2 exposes individuals to significant risk".into());
                            }
                        },
                        None => {
                            let message = "delta: the number of records must be known to check if delta is a value that satisfies differential privacy";
                            if strict_delta_check {
                                return Err(message.into())
                            }
                            warnings.push(message.into());
                        }
                    }
                }
            }
        }
    };

    Ok(warnings)
}

pub fn get_epsilon(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::Approximate(distance) => Ok(distance.epsilon),
//        _ => Err("epsilon is not defined".into())
    }
}

pub fn get_delta(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::Approximate(distance) => Ok(distance.delta),
        // _ => Err("delta is not defined".into())
    }
}

pub fn broadcast_privacy_usage(usages: &[proto::PrivacyUsage], length: usize) -> Result<Vec<proto::PrivacyUsage>> {
    if usages.len() == length {
        return Ok(usages.to_owned());
    }

    if usages.len() != 1 {
        if length != 1 {
            bail!("{} privacy parameters passed when either one or {} was required", usages.len(), length);
        } else {
            bail!("{} privacy parameters passed when one was required", usages.len());
        }
    }

    Ok(match usages[0].distance.clone().ok_or("distance must be defined on a privacy usage")? {
        proto::privacy_usage::Distance::Approximate(approx) => (0..length)
            .map(|_| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: approx.epsilon / (length as f64),
                    delta: approx.delta / (length as f64),
                }))
            }).collect()
    })
}