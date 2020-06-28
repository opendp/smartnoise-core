use std::collections::{HashMap, HashSet};
use crate::errors::*;
use itertools::Itertools;
use std::cmp::Ordering;
use crate::{proto, Float};
use crate::base::{ValueProperties, Release, GroupId, IndexKey};
use crate::components::Mechanism;
use crate::utilities::{get_input_properties, get_common_value, get_dependents};

type BatchIdentifier = (u32, u32);
type PartitionIds = Vec<u32>;

fn compute_batch_privacy_usage(
    privacy_usages: Vec<&proto::PrivacyUsage>
) -> Result<proto::PrivacyUsage> {
    // TODO: insert advanced composition here
    //    This is just linear composition
    privacy_usages.into_iter().cloned().map(Ok)
        .fold1(|l, r| l? + r?)
        .unwrap_or_else(|| Ok(proto::PrivacyUsage {
            distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: 0.,
                delta: 0.,
            }))
        }))
}

/// Use a computation graph to partition privacy usages into batches.
///
/// This algorithm takes into account dynamic graph submissions that require multiple batches to compute.
/// This algorithm traverses up to and stops at partition ids.
///     The partition ids are returned as a second argument.
fn batch_partition<'a>(
    graph: &HashMap<u32, proto::Component>,
    privacy_usages: &'a HashMap<u32, Vec<proto::PrivacyUsage>>,
) -> Result<(HashMap<BatchIdentifier, Vec<&'a proto::PrivacyUsage>>, PartitionIds)> {

    // contains the subgraph for each submission id
    let mut submissions = HashMap::<u32, HashMap<u32, proto::Component>>::new();

    // populate/create the submissions partitioning
    graph.iter()
        .for_each(|(node_id, component)| {
            submissions
                .entry(component.submission)
                .or_insert_with(HashMap::new)
                .insert(*node_id, component.clone());
        });

    // each batch is identified by the (submission_id, dependency_id),
    //    where the dependency_id is the maximum number of releases prior to a node id in the batch
    let mut batches = HashMap::<BatchIdentifier, Vec<&proto::PrivacyUsage>>::new();

    // node ids of partitions - these will require special treatment, and are not yet counted
    let mut partition_ids = Vec::new();

    // populate/create the batches partitioning
    submissions.into_iter()
        .try_for_each(|(submission_id, subgraph)| {
            // for any node id in the submission, list all nodes that use it
            //    we are traversing backwards through the graph, starting from materialize/literal nodes
            let parents = get_dependents(&subgraph);

            // contains a record of how many releases are present before any given node id within the submission
            //     {node_id: dependency_id}
            let mut dependency_ids = HashMap::<u32, u32>::new();

            // find partition ids
            let mut blacklist = HashSet::new();
            let mut blacklist_traversal = graph.iter()
                // filter to partition components
                .filter(|(_, component)|
                    matches!(component.variant.as_ref().unwrap(), proto::component::Variant::Partition(_)))
                // insert partition components into blacklist and start traversal at the dependents
                .map(|(id, _)| {
                    blacklist.insert(*id);
                    partition_ids.push(*id);
                    parents.get(id).cloned().unwrap_or_else(HashSet::new)
                })
                .flatten()
                .collect::<Vec<u32>>();

            // consume the entire graph after the partition node
            while !blacklist_traversal.is_empty() {
                let node_id = blacklist_traversal.pop().unwrap();
                blacklist.insert(node_id);

                let component = match graph.get(&node_id) {
                    Some(component) => component,
                    None => continue
                };
                component.arguments().values()
                    .filter(|id| !blacklist.contains(id))
                    .for_each(|id| blacklist_traversal.push(*id));

                if let Some(ids) = parents.get(&node_id) {
                    ids.iter()
                        .filter(|id| !blacklist.contains(id))
                        .for_each(|id| blacklist_traversal.push(*id));
                }
            }

            // start traversal from all source nodes in the submission
            //    and set them as dependency_id zero (there are no releases before roots)
            let mut traversal = subgraph.iter()
                .filter(|(id, component)|
                    !blacklist.contains(id) && component.arguments().values()
                        .filter(|id| subgraph.contains_key(id))
                        .count() == 0
                )
                // (node_id, dependency_id)
                .map(|(id, _)| (*id, 0))
                .collect::<Vec<BatchIdentifier>>();

            // determine how many releases are necessary to reach each node id in the submission
            while !traversal.is_empty() {
                let (node_id, mut dependency_id) = traversal.pop().unwrap();

                if blacklist.contains(&node_id) {
                    continue;
                }
                blacklist.insert(node_id);

                // update the dependency_id- if statically checking and submitting,
                //     what is the minimum number of times the graph would need to be submitted to reach this node?
                {
                    let prior_value = dependency_ids
                        .entry(node_id).or_insert(dependency_id);
                    *prior_value = *prior_value.max(&mut dependency_id);
                }

                // split parents into new batch anytime privacy is used
                if privacy_usages.contains_key(&node_id) {
                    dependency_id += 1;
                }

                // add all parents to the traversal, with the number of releases needed to reach this point
                if let Some(pars) = parents.get(&node_id) {
                    traversal.extend(pars.iter().map(|v| (*v, dependency_id)));
                }
            }

            // use the dependency structure discovered in the traversal to enumerate the batches present in this submission
            let mut dependency_batches = HashMap::<u32, HashSet<u32>>::new();
            dependency_ids.iter()
                .for_each(|(node_id, dependency_id)| {
                    dependency_batches.entry(*dependency_id)
                        .or_insert_with(HashSet::new)
                        .insert(*node_id);
                });

            // we now have batches for the subgraph added by a specific submission, represented as a hashmap of hashsets of node ids
            // insert each batch into the larger top-level batch listing
            dependency_batches.into_iter().for_each(|(dependency_id, batch_values)| {
                batches.insert((submission_id, dependency_id), batch_values.iter()
                    .map(|node_id| privacy_usages.get(node_id))
                    .flatten().flatten()
                    .collect::<Vec<&'a proto::PrivacyUsage>>());
            });
            Ok::<_, Error>(())
        })?;

    Ok((batches, partition_ids))
}

/// Compute the privacy usage of a graph,
///     based on the privacy definition
///     and actual usages reported by any computed values.
pub fn compute_graph_privacy_usage(
    graph: &HashMap<u32, proto::Component>,
    privacy_definition: &proto::PrivacyDefinition,
    properties: &HashMap<u32, ValueProperties>,
    release: &Release,
) -> Result<proto::PrivacyUsage> {

    // compute the privacy usage for every node in the graph
    //    include updated privacy usages for nodes that have already been released and may have actually consumed a different amount
    let release_privacy_usages = graph.iter()
        .map(|(node_id, component)| Ok((*node_id, component.get_privacy_usage(
            &privacy_definition,
            release.get(node_id)
                .and_then(|v| v.privacy_usages.as_ref()),
            &get_input_properties(component, &properties)?)?))
        )
        .collect::<Result<Vec<(u32, Option<Vec<proto::PrivacyUsage>>)>>>()?
        .into_iter().filter_map(|(node_id, usages)| Some((node_id, usages?)))
        .collect::<HashMap<u32, Vec<proto::PrivacyUsage>>>();

    // for any node id in the submission, list all nodes that use it
    let dependent_edges = get_dependents(graph);

    // partition the graph into batches,
    //     where batches are identified by the submission id and number of releases prior to the node within the batch (dependency id)
    //     also return the node ids of partitions, as parallel composition needs to be applied to its dependents
    let (batches, partition_ids) = batch_partition(graph, &release_privacy_usages)?;

    let zero_usage = || proto::PrivacyUsage {
        distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
            epsilon: 0.0,
            delta: 0.0,
        }))
    };

    // get all node ids that are indexed by a specific category
    let get_category_indexes = |
        category: IndexKey, partition_id: u32,
    | -> Result<Vec<u32>> {
        let dependents = match dependent_edges.get(&partition_id) {
            Some(dependents) => dependents,
            None => return Ok(vec![])
        };
        // retrieve the indexes into the partition
        Ok(dependents.iter()
            // for each index, check if their column name (category) is the same as the category in the signature
            .map(|index_id| Ok((
                *index_id, category == IndexKey::new(release.get(graph.get(index_id).unwrap()
                    .arguments().get(&IndexKey::from("names"))
                    .ok_or_else(|| "names argument must be specified on an index into partitions")?)
                    .ok_or_else(|| "names value must be defined")?.value.clone().array()?)?)))

            // return if an error was encountered
            .collect::<Result<Vec<(u32, bool)>>>()?.iter()

            // collect the index ids of the indexes whose category matched the category in the signature
            .filter(|pair| pair.1).map(|pair| pair.0)
            .collect::<Vec<u32>>())
    };

    // get all node ids that are dependents of a specific node_id
    let get_downstream_ids = |
        category: Option<IndexKey>, node_id: u32
    | -> Result<HashSet<u32>> {
        let mut downstream_ids = HashSet::new();
        let mut traversal = vec![node_id];
        while !traversal.is_empty() {
            let node_id = traversal.pop().unwrap();

            if let Some(category) = &category {
                if let Some(proto::component::Variant::Union(x)) = graph.get(&node_id).and_then(|v| v.variant.as_ref()) {
                    if !x.flatten {
                        // the only downstream nodes from the partition are the ones that match the same partition
                        traversal.extend(get_category_indexes(category.clone(), node_id)?);
                        downstream_ids.insert(node_id);
                        continue
                    }
                }
            }

            if let Some(dependents) = dependent_edges.get(&node_id)  {
                traversal.extend(dependents);
            }
            downstream_ids.insert(node_id);
        }
        Ok(downstream_ids)
    };

    // get the subset of a graph downstream of a specific node id
    let get_downstream_graph = |
        category: Option<IndexKey>, node_id: u32
    | -> Result<HashMap<u32, proto::Component>> {
        Ok(get_downstream_ids(category, node_id)?.iter()
            .map(|node_id| (*node_id, graph.get(node_id).unwrap().clone()))
            .collect::<HashMap<u32, proto::Component>>())
    };

    // return the max of the left and right privacy usages
    let max_usage = |l: Result<proto::PrivacyUsage>, r: Result<proto::PrivacyUsage>| -> Result<proto::PrivacyUsage> {
        let proto::privacy_usage::DistanceApproximate {
            epsilon: eps_l, delta: del_l
        } = match l?.distance {
            Some(proto::privacy_usage::Distance::Approximate(x)) => Ok(x),
            _ => Err("expected approximate privacy")
        }?;
        let proto::privacy_usage::DistanceApproximate {
            epsilon: eps_r, delta: del_r
        } = match r?.distance {
            Some(proto::privacy_usage::Distance::Approximate(x)) => Ok(x),
            _ => Err("expected approximate privacy")
        }?;

        Ok(proto::PrivacyUsage {
            distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: eps_l.max(eps_r),
                delta: del_l.max(del_r),
            }))
        })
    };

    // compute privacy usage of a subset of the graph,
    //     where the subset is indicated by a collection of node ids
    let compute_all_partitions_usage = |
        partition_ids: Vec<u32>
    | -> Result<proto::PrivacyUsage> {
        partition_ids.iter()
            .map(|partition_id| compute_graph_privacy_usage(
                &get_downstream_graph(None, *partition_id)?,
                privacy_definition, properties, release))
            .fold1(max_usage)
            .unwrap_or_else(|| Ok(zero_usage()))
    };

    // compute the overall privacy usage
    let partitions_usage: proto::PrivacyUsage = partition_ids.into_iter()
        // for each partition component...
        .map(|partition_node_id| {
            let partition_properties = properties.get(&partition_node_id)
                .ok_or_else(|| "partition properties must be defined")?;

            partition_properties.partitions()?.children.keys()
                // for each category/part in the partition...
                .map(|category| {
                    let unioned_downstream_graph = get_category_indexes(category.clone(), partition_node_id)?.iter()
                        // for each index into the category...
                        .map(|index_id| get_downstream_graph(Some(category.clone()), *index_id))
                        .collect::<Result<Vec<_>>>()?.into_iter().flatten()
                        .collect::<HashMap<u32, proto::Component>>();

                    let (batches, partition_ids) = batch_partition(
                        &unioned_downstream_graph, &release_privacy_usages)?;
                    let batch_usages = batches.into_iter()
                        .map(|(_, batch)| compute_batch_privacy_usage(batch))
                        .fold1(|l, r| l? + r?)
                        .unwrap_or_else(|| Ok(zero_usage()))?;

                    batch_usages + compute_all_partitions_usage(partition_ids)?
                })
                .fold1(max_usage)
                .unwrap_or_else(|| Ok(zero_usage()))
        })
        .fold1(|l, r| l? + r?)
        .unwrap_or_else(|| Ok(zero_usage()))?;

    let batch_usages = batches.into_iter()
        .map(|(_, batch)| compute_batch_privacy_usage(batch))
        .fold1(|l, r| l? + r?)
        .unwrap_or_else(|| Ok(zero_usage()))?;

    batch_usages + partitions_usage
}

// pub fn privacy_usage_reducer(
//     left: &proto::PrivacyUsage,
//     right: &proto::PrivacyUsage,
//     operator: fn(f64, f64) -> f64,
// ) -> proto::PrivacyUsage {
//     use proto::privacy_usage::Distance as Distance;
//
//     proto::PrivacyUsage {
//         distance: match (left.distance.to_owned().unwrap(), right.distance.to_owned().unwrap()) {
//             (Distance::Approximate(x), Distance::Approximate(y)) => Some(Distance::Approximate(proto::privacy_usage::DistanceApproximate {
//                 epsilon: operator(x.epsilon, y.epsilon),
//                 delta: operator(x.delta, y.delta),
//             }))
//         }
//     }
// }


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
                        return Err("delta: must be smaller than one".into());
                    }
                    match num_records {
                        Some(num_records) => {
                            if usage.delta * num_records as f64 > 1.0 {
                                return Err("delta: a value greater than 1 / num_records is not differentially private".into());
                            }

                            if usage.delta * num_records.pow(2) as f64 > 1.0 {
                                warnings.push("delta: a value greater than 1 / num_records^2 exposes individuals to significant risk".into());
                            }
                        }
                        None => {
                            let message = "delta: the number of records must be known to check if delta is a value that satisfies differential privacy";
                            if strict_delta_check {
                                return Err(message.into());
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

pub fn spread_privacy_usage(usages: &[proto::PrivacyUsage], length: usize) -> Result<Vec<proto::PrivacyUsage>> {
    if usages.len() == length {
        return Ok(usages.to_owned());
    }

    if usages.len() != 1 {
        if length != 1 {
            bail!("{} privacy parameters passed when either 1 or {} was required", usages.len(), length);
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

pub fn get_group_id_path(arguments: Vec<Vec<GroupId>>) -> Result<Vec<GroupId>> {
    let partition_depth = get_common_value(&arguments.iter()
        .map(|group_ids| group_ids.len())
        .collect())
        .ok_or_else(|| "all arguments must be parts of the same partition")?;

    if partition_depth == 0 {
        return Err("arguments must come from a partition".into());
    }

    (0..partition_depth - 1)
        .map(|depth| get_common_value(&arguments.iter()
            .map(|group_ids| group_ids[depth].clone())
            .collect()
        ))
        .collect::<Option<Vec<GroupId>>>()
        .ok_or_else(|| "partition paths of all arguments must match".into())
}

pub fn get_c_stability_multiplier(arguments: Vec<Vec<GroupId>>) -> Result<Float> {
    let partition_depth = get_common_value(&arguments.iter()
        .map(|group_ids| group_ids.len())
        .collect())
        .ok_or_else(|| "all arguments must be parts of the same partition")?;

    if arguments.is_empty() {
        return Err("c-stability cannot be determined on an empty argument set".into());
    }
    if partition_depth == 0 {
        return Ok(1.);
    }

    if partition_depth > 1 && !(0..partition_depth - 1).all(|depth|
        get_common_value(&arguments.iter()
            .map(|group_ids| group_ids[depth].clone())
            .collect()
        ).is_some()) {
        return Err("all arguments must be parts of the same partition".into());
    }

    let group_ids = arguments.into_iter()
        .map(|group_id| group_id.last().unwrap().clone())
        .collect::<Vec<GroupId>>();

    get_common_value(&group_ids.iter()
        .map(|group_id| group_id.partition_id).collect())
        .ok_or_else(|| "all arguments must be parts of the same partition")?;

    let mut counts = HashMap::new();
    group_ids.into_iter().for_each(|group_id|
        *counts.entry(group_id.index).or_insert(0) += 1);

    Ok(*counts.values().max().unwrap() as Float)
}