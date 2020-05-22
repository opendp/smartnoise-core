use std::collections::{HashMap, HashSet};
use crate::errors::*;
use itertools::Itertools;
use std::cmp::Ordering;
use crate::proto;
use crate::base::{ValueProperties, Release, Indexmap, Value};
use crate::components::Mechanism;
use crate::utilities::get_input_properties;


fn get_dependents(graph: &HashMap<u32, proto::Component>) -> HashMap<u32, HashSet<u32>> {
    let mut dependents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        component.arguments.values().for_each(|source_node_id| {
            dependents
                .entry(*source_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });
    dependents
}

fn compute_batch_privacy_usage(
    privacy_usages: Vec<&proto::PrivacyUsage>
) -> Result<proto::PrivacyUsage> {
    // TODO: insert advanced composition here
    //    This is just linear composition
    privacy_usages.into_iter().cloned().map(Ok)
        .fold1(|l, r| l? + r?)
        .ok_or_else(|| Error::from("batch may not be empty"))?
}

fn batch_partition<'a>(
    graph: &HashMap<u32, proto::Component>,
    privacy_usages: &'a HashMap<u32, Vec<proto::PrivacyUsage>>,
) -> Result<(HashMap<(u32, u32), Vec<&'a proto::PrivacyUsage>>, Vec<u32>)> {

    // contains the subgraph for each submission id
    let mut submissions = HashMap::<u32, HashMap<u32, proto::Component>>::new();

    graph.iter()
        .for_each(|(node_id, component)| {
            submissions
                .entry(component.submission)
                .or_insert_with(HashMap::new)
                .insert(*node_id, component.clone());
        });


    let mut batches = HashMap::<(u32, u32), Vec<&proto::PrivacyUsage>>::new();
    let mut partition_ids = Vec::new();
    submissions.into_iter()
        .map(|(submission_id, subgraph)| {
            let parents = get_dependents(&subgraph);

            // {node_id: dependency_id}
            let mut dependency_ids = HashMap::<u32, u32>::new();

            // get all source nodes within the submission, and set them as dependency_id zero
            let mut traversal = subgraph.iter()
                .filter(|(_, component)|
                    component.arguments.values().any(|id| subgraph.contains_key(id)))
                // (id, dependency_id)
                .map(|(id, _)| (*id, 0))
                .collect::<Vec<(u32, u32)>>();

            while !traversal.is_empty() {
                let (node_id, mut dependency_id) = traversal.pop().unwrap();

                let component = subgraph.get(&node_id).unwrap();

                // terminate at partition nodes
                if let proto::component::Variant::Partition(_) = component.variant
                    .as_ref().ok_or_else(|| "variant: must be defined")? {
                    partition_ids.push(node_id);
                    continue;
                }

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
                traversal.extend(parents.get(&node_id).unwrap().iter()
                    .map(|v| (*v, dependency_id)))
            }

            // construct a representation of the node ids in each submission partitioned by the dependency structure
            //    there should be as few batches as possible to take full advantage of advanced composition
            let mut dependency_batches = HashMap::<u32, HashSet<u32>>::new();
            dependency_ids.iter()
                .for_each(|(node_id, dependency_id)| {
                    dependency_batches.entry(*dependency_id)
                        .or_insert_with(HashSet::new)
                        .insert(*node_id);
                });
            // insert the partitioned node ids as individual batches
            dependency_batches.into_iter().for_each(|(dependency_id, batch_values)| {
                batches.insert((submission_id, dependency_id), batch_values.iter()
                    .map(|node_id| privacy_usages.get(node_id).unwrap())
                    .flatten()
                    .collect::<Vec<&'a proto::PrivacyUsage>>());
            });
            Ok(())
        }).collect::<Result<()>>()?;

    Ok((batches, partition_ids))
}

pub fn compute_graph_privacy_usage(
    graph: &HashMap<u32, proto::Component>,
    privacy_definition: &proto::PrivacyDefinition,
    properties: &HashMap<u32, ValueProperties>,
    release: &Release,
) -> Result<proto::PrivacyUsage> {
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

    let dependent_edges = get_dependents(graph);

    let (batches, partition_ids) = batch_partition(graph, &release_privacy_usages)?;

    let zero_usage = || proto::PrivacyUsage {
        distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
            epsilon: 0.0,
            delta: 0.0,
        }))
    };

    let get_category_indexes = |
        category: Value,
        partition_id: u32,
    | -> Result<Vec<u32>> {

        Ok(dependent_edges
            // retrieve the indexes into the partition
            .get(&partition_id)
            .ok_or_else(|| "partition dependencies not found")?.iter()

            // for each index, check if their column name (category) is the same as the category in the signature
            .map(|index_id| Ok((
                *index_id, category == release.get(graph.get(index_id).unwrap()
                    .arguments.get("columns")
                    .ok_or_else(|| "columns must be specified on an index")?)
                    .ok_or_else(|| "columns value must be defined")?.value)))

            // return if an error was encountered
            .collect::<Result<Vec<(u32, bool)>>>()?.iter()

            // collect the index ids of the indexes whose category matched the category in the signature
            .filter(|pair| pair.1).map(|pair| pair.0)
            .collect::<Vec<u32>>())
    };

    let get_downstream_ids = |
        node_id: u32
    | -> Result<HashSet<u32>> {
        let mut downstream_ids = HashSet::new();
        let mut traversal = vec![node_id];
        while !traversal.is_empty() {
            let node_id = traversal.pop().unwrap();
            downstream_ids.insert(node_id);
            traversal.extend(dependent_edges.get(&node_id).unwrap())
        }
        Ok(downstream_ids)
    };

    let get_downstream_graph = |
        node_id: u32
    | -> Result<HashMap<u32, proto::Component>> {
        Ok(get_downstream_ids(node_id)?.iter()
            .map(|node_id| (*node_id, graph.get(node_id).unwrap().clone()))
            .collect::<HashMap<u32, proto::Component>>())
    };

    let max_usage = |
        l: Result<proto::PrivacyUsage>, r: Result<proto::PrivacyUsage>
    | -> Result<proto::PrivacyUsage> {
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
                delta: del_l.max(del_r)
            }))
        })
    };

    let compute_partition_usage = |
        partition_ids: Vec<u32>
    | -> Result<proto::PrivacyUsage> {
        partition_ids.iter()
            .map(|partition_id| compute_graph_privacy_usage(
                &get_downstream_graph(*partition_id)?, privacy_definition, properties, release,
            ))
            .fold1(max_usage)
            .unwrap_or_else(|| Ok(zero_usage()))
    };

    let partitions_usage: proto::PrivacyUsage = partition_ids.into_iter().map(|partition_node_id| {
        let partition_properties = properties.get(&partition_node_id)
            .ok_or_else(|| "partition properties must be defined")?;

        match &partition_properties.indexmap()?.properties {
            Indexmap::Str(partitions) => partitions.keys()
                .map(|category| get_category_indexes(category.clone().into(), partition_node_id)?.iter()
                    .map(|index_id| {
                        let (batches, partition_ids) = batch_partition(
                            &get_downstream_graph(*index_id)?, &release_privacy_usages)?;
                        let batch_usages = batches.into_iter()
                            .map(|(_, batch)| compute_batch_privacy_usage(batch))
                            .fold1(|l, r| l? + r?)
                            .unwrap_or_else(|| Ok(zero_usage()))?;

                        batch_usages + compute_partition_usage(partition_ids)?
                    })
                    // sum all indexes into the category
                    .fold1(|l, r| l? + r?)
                    .unwrap_or_else(|| Ok(zero_usage())))
                .fold1(max_usage)
                .unwrap_or_else(|| Ok(zero_usage())),
            _ => panic!("TODO: once str is debugged"),
            // Indexmap::Bool(indexmap) => indexmap.keys(),
            // Indexmap::I64(indexmap) => indexmap.keys()
        }
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