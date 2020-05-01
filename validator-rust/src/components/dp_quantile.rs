use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};


use crate::base::{NodeProperties, Value, Array};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_column};


impl Expandable for proto::DpQuantile {
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

        let data_id = *component.arguments.get("data")
            .ok_or_else(|| Error::from("data is a required argument to DPMedian"))?;

        if self.mechanism.to_lowercase().as_str() == "exponential" {
            computation_graph.insert(*maximum_id, proto::Component {
                arguments: hashmap!["data".to_owned() => data_id],
                variant: Some(proto::component::Variant::ExponentialMechanism(Box::new(proto::ExponentialMechanism {
                    aggregator: Some(Box::new(proto::Component {
                        arguments: hashmap!["data".to_owned() => data_id],
                        variant: Some(proto::component::Variant::Quantile(proto::Quantile {
                            alpha: self.alpha,
                            interpolation: self.interpolation.clone(),
                        })),
                        omit: true,
                        batch: component.batch,
                    })),
                    privacy_usage: self.privacy_usage.clone(),
                }))),
                omit: true,
                batch: component.batch,
            });

            Ok(proto::ComponentExpansion {
                computation_graph,
                properties: HashMap::new(),
                releases: HashMap::new(),
                traversal: Vec::new(),
            })
        } else {
            // quantile
            current_id += 1;
            let id_median = current_id;
            computation_graph.insert(id_median, proto::Component {
                arguments: hashmap!["data".to_owned() => data_id],
                variant: Some(proto::component::Variant::Quantile(proto::Quantile {
                    alpha: 0.5,
                    interpolation: self.interpolation.clone(),
                })),
                omit: true,
                batch: component.batch,
            });

            // sanitizing
            computation_graph.insert(component_id.clone(), proto::Component {
                arguments: hashmap![
			    "data".to_owned() => id_median
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
                batch: component.batch,
            });

            Ok(proto::ComponentExpansion {
                computation_graph,
                properties: HashMap::new(),
                releases: HashMap::new(),
                traversal: vec![id_median],
            })
        }
    }
}


impl Report for proto::DpQuantile {
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

        let minimums = data_property.lower_f64().unwrap();
        let maximums = data_property.upper_f64().unwrap();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMedian".to_string(),
                variables: serde_json::json!(variable_name),
                release_info: match release.array()? {
                    Array::F64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    Array::I64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    _ => return Err("maximum must be numeric".into())
                },
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
                        "constraint": {
                            "lowerbound": minimums[column_number],
                            "upperbound": maximums[column_number]
                        }
                    }),
                },
            });
        }
        Ok(Some(releases))
    }
}
