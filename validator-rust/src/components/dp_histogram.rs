use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};
use ndarray::{arr0};

use crate::base::{NodeProperties, Value};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, get_ith_column, get_literal, privacy::broadcast_privacy_usage};


impl Expandable for proto::DpHistogram {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let data_id = component.arguments.get("data")
            .ok_or_else(|| Error::from("data is a required argument to DPHistogram"))?.to_owned();

        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        // histogram
        maximum_id += 1;
        let id_histogram = maximum_id;
        let mut histogram_arguments = hashmap!["data".to_owned() => data_id];
        component.arguments.get("categories")
            .map(|v| histogram_arguments.insert("categories".to_string(), *v));
        component.arguments.get("null_value")
            .map(|v| histogram_arguments.insert("null_value".to_string(), *v));
        component.arguments.get("edges")
            .map(|v| histogram_arguments.insert("edges".to_string(), *v));
        component.arguments.get("inclusive_left")
            .map(|v| histogram_arguments.insert("inclusive_left".to_string(), *v));
        computation_graph.insert(id_histogram, proto::Component {
            arguments: histogram_arguments,
            variant: Some(proto::component::Variant::Histogram(proto::Histogram {})),
            omit: true,
            submission: component.submission,
        });

        if self.mechanism.to_lowercase().as_str() == "simplegeometric" {
            let id_upper = match component.arguments.get("upper") {
                Some(id) => id.clone(),
                None => {
                    let count_max = match data_property.num_records {
                        Some(num_records) => arr0(num_records).into_dyn(),
                        None => match self.enforce_constant_time {
                            true => return Err("upper must be set when enforcing constant time".into()),
                            false => arr0(std::i64::MAX).into_dyn()
                        }
                    };
                    // count_max
                    maximum_id += 1;
                    let max_id = maximum_id;
                    let (patch_node, count_max_release) = get_literal(count_max.into(), &component.submission)?;
                    computation_graph.insert(max_id.clone(), patch_node);
                    releases.insert(max_id.clone(), count_max_release);
                    max_id
                }
            };

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: hashmap![
                    "data".to_owned() => id_histogram,
                    "lower".to_owned() => *component.arguments.get("lower")
                        .ok_or_else(|| Error::from("lower must be provided as an argument"))?,
                    "upper".to_owned() => id_upper
                ],
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    enforce_constant_time: false
                })),
                omit: false,
                submission: component.submission,
            });
        } else {

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: hashmap![
                    "data".to_owned() => id_histogram
                ],
                variant: Some(match self.mechanism.to_lowercase().as_str() {
                    "laplace" => proto::component::Variant::LaplaceMechanism(proto::LaplaceMechanism {
                        privacy_usage: self.privacy_usage.clone()
                    }),
                    "gaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                        privacy_usage: self.privacy_usage.clone()
                    }),
                    _ => panic!("Unexpected invalid token {:?}", self.mechanism.as_str()),
                }),
                omit: false,
                submission: component.submission,
            });
        }


        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: vec![id_histogram],
            warnings: vec![]
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
        variable_names: Option<&Vec<String>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPHistogram".to_string(),
                variables: serde_json::json!(variable_name),
                // extract ith column of release
                release_info: value_to_json(&get_ith_column(
                    release.array()?.i64()?,
                    &column_number
                )?.into())?,
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                submission: component.submission as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.mechanism.clone(),
                    argument: serde_json::json!({}),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
