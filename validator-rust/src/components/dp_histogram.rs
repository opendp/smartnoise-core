use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};
use ndarray::{arr0};

use crate::base::{NodeProperties, Value};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_release, get_literal};


impl Expandable for proto::DpHistogram {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut data_id = component.arguments.get("data")
            .ok_or_else(|| Error::from("data is a required argument to DPHistogram"))?.to_owned();

        let data_property = properties.get("data")
                                        .ok_or("data: missing")?.array()
                                        .map_err(prepend("data:"))?;

        let count_max_id = match component.arguments.get("count_max") {
            Some(id) => id.clone(),
            None => {
                let count_max = match data_property.num_records {
                    Some(num_records) => arr0(num_records).into_dyn(),
                    None => match self.enforce_constant_time {
                        true => return Err("count_max must be set when enforcing constant time".into()),
                        false => arr0(std::i64::MAX).into_dyn()
                    }
                };
                // count_max
                maximum_id += 1;
                let id_count_max = maximum_id;
                let (patch_node, count_max_release) = get_literal(&count_max.into(), &component.batch)?;
                computation_graph.insert(id_count_max.clone(), patch_node);
                releases.insert(id_count_max.clone(), count_max_release);
                id_count_max
            }
        };

        let mut traversal = Vec::<u32>::new();
        match (component.arguments.get("edges"), component.arguments.get("categories")) {

            (Some(edges_id), None) => {
                // digitize
                let null_id = component.arguments.get("null_value")
                    .ok_or_else(|| Error::from("null_value is a required argument to DPHistogram"))?;
                let inclusive_left_id = component.arguments.get("inclusive_left")
                    .ok_or_else(|| Error::from("inclusive_left is a required argument to DPHistogram when categories are not known"))?;
                maximum_id += 1;
                let id_digitize = maximum_id;
                computation_graph.insert(id_digitize, proto::Component {
                    arguments: hashmap![
                        "data".to_owned() => data_id,
                        "edges".to_owned() => *edges_id,
                        "null_value".to_owned() => *null_id,
                        "inclusive_left".to_owned() => *inclusive_left_id
                    ],
                    variant: Some(proto::component::Variant::from(proto::Digitize {})),
                    omit: true,
                    batch: component.batch,
                });
                data_id = id_digitize;
                traversal.push(id_digitize);
            }

            (None, Some(categories_id)) => {
                // clamp

                let null_id = component.arguments.get("null_value")
                    .ok_or_else(|| Error::from("null_value is a required argument to DPHistogram when categories are not known"))?;
                maximum_id += 1;
                let id_clamp = maximum_id;
                computation_graph.insert(id_clamp, proto::Component {
                    arguments: hashmap![
                        "data".to_owned() => data_id,
                        "categories".to_owned() => *categories_id,
                        "null_value".to_owned() => *null_id
                    ],
                    variant: Some(proto::component::Variant::from(proto::Clamp {})),
                    omit: true,
                    batch: component.batch,
                });
                data_id = id_clamp;
                traversal.push(id_clamp);
            }

            (None, None) => {
                let data_property = properties.get("data")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                if data_property.categories().is_err() {
                    return Err("either edges or categories must be supplied".into())
                }
            }
            _ => return Err("either edges or categories must be supplied".into())
        }

        // histogram
        maximum_id += 1;
        let id_histogram = maximum_id;
        computation_graph.insert(id_histogram, proto::Component {
            arguments: hashmap!["data".to_owned() => data_id],
            variant: Some(proto::component::Variant::from(proto::Histogram {})),
            omit: true,
            batch: component.batch,
        });
        traversal.push(id_histogram);

        // noising
        computation_graph.insert(*component_id, proto::Component {
            arguments: hashmap![
                "data".to_owned() => id_histogram,
                "min".to_owned() => *component.arguments.get("min")
                    .ok_or_else(|| Error::from("min must be provided as an argument"))?,
                "max".to_owned() => count_max_id
            ],
            variant: Some(proto::component::Variant::from(proto::SimpleGeometricMechanism {
                privacy_usage: self.privacy_usage.clone(),
                enforce_constant_time: false
            })),
            omit: false,
            batch: component.batch,
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal
        })
    }
}

impl Report for proto::DpHistogram {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value,
        variable_names: &Vec<String>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..num_columns {
            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPHistogram".to_string(),
                variables: serde_json::json!(variable_names),
                // extract ith column of release
                release_info: value_to_json(&get_ith_release(
                    release.array()?.i64()?,
                    &(column_number as usize)
                )?.into())?,
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number as usize].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.implementation.clone(),
                    argument: serde_json::json!({}),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
