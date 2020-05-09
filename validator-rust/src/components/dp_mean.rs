use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};

use crate::base::{NodeProperties, Value};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_column};
use serde_json;


impl Expandable for proto::DpMean {
    /// Expand component
    /// # Arguments
    /// * `&self` - this
    /// * `_privacy_definition` - privacy definition from protocol buffer descriptor
    /// * `component` - component from prototypes/components.proto
    /// * `_properties` - NodeProperties
    /// * `component_id` - identifier for component from prototypes/components.proto
    /// * `maximum_id` - last ID value created for sequence, increement used to define current ID
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        if self.implementation.to_lowercase().as_str() == "plug-in" {
            // mean
            current_id += 1;
            let id_dp_sum = current_id;
            computation_graph.insert(id_dp_sum, proto::Component {
                arguments: hashmap!["data".to_owned() => *component.arguments.get("data")
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?],
                variant: Some(proto::component::Variant::DpSum(proto::DpSum {
                    mechanism: self.mechanism.clone(),
                    privacy_usage: self.privacy_usage.iter().cloned().map(|v| v / 2.)
                        .collect::<Result<Vec<proto::PrivacyUsage>>>()?
                })),
                omit: true,
                batch: component.batch,
            });

            current_id += 1;
            let id_dp_count = current_id;
            computation_graph.insert(id_dp_count, proto::Component {
                arguments: hashmap!["data".to_owned() => *component.arguments.get("data")
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?],
                variant: Some(proto::component::Variant::DpCount(proto::DpCount {
                    enforce_constant_time: false,
                    mechanism: self.mechanism.clone(),
                    privacy_usage: self.privacy_usage.iter().cloned().map(|v| v / 2.)
                        .collect::<Result<Vec<proto::PrivacyUsage>>>()?
                })),
                omit: true,
                batch: component.batch,
            });

            computation_graph.insert(*component_id, proto::Component {
                arguments: hashmap!["left".to_owned() => id_dp_sum, "right".to_owned() => id_dp_count],
                variant: Some(proto::component::Variant::Divide(proto::Divide {})),
                omit: true,
                batch: component.batch,
            });

            Ok(proto::ComponentExpansion {
                computation_graph,
                properties: HashMap::new(),
                releases: HashMap::new(),
                traversal: vec![id_dp_count, id_dp_sum]
            })
        }

        else if self.implementation.to_lowercase().as_str() == "resize" {
            // mean
            current_id += 1;
            let id_mean = current_id;
            computation_graph.insert(id_mean, proto::Component {
                arguments: hashmap!["data".to_owned() => *component.arguments.get("data")
                .ok_or_else(|| Error::from("data must be provided as an argument"))?],
                variant: Some(proto::component::Variant::Mean(proto::Mean {})),
                omit: true,
                batch: component.batch,
            });

            // noising
            computation_graph.insert(component_id.clone(), proto::Component {
                arguments: hashmap!["data".to_owned() => id_mean],
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
                batch: component.batch,
            });

            Ok(proto::ComponentExpansion {
                computation_graph,
                properties: HashMap::new(),
                releases: HashMap::new(),
                traversal: vec![id_mean]
            })
        }

        else {
            bail!("`{}` is not recognized as a valid implementation. Must be one of [`resize`, `plug-in`]")
        }
    }
}

impl Report for proto::DpMean {
    /// summarize results
    /// # Arguments
    /// * `&self` - this
    /// * `node_id` - identifier for node
    /// * `component` - component from prototypes/components.proto
    /// * `public_arguments` - HashMap of String, Value public arguments
    /// * `properties` - NodeProperties
    /// * `release` - JSONRelease containing DP release information
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

        let lower = data_property.lower_f64()?;
        let upper = data_property.upper_f64()?;
        let num_records = data_property.num_records()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMean".to_string(),
                variables: serde_json::json!(variable_name),
                release_info: value_to_json(&get_ith_column(
                    release.array()?.f64()?,
                    &(column_number as usize)
                )?.into())?,
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.mechanism.clone(),
                    argument: serde_json::json!({
                        // TODO: AlgorithmInfo -> serde_json::Value, move implementation into algorithm_info
                        "implementation": self.implementation.clone(),
                        "n": num_records,
                        "constraint": {
                            "lowerbound": lower[column_number],
                            "upperbound": upper[column_number]
                        }
                    })
                }
            });
        }
        Ok(Some(releases))
    }
}
