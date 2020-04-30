use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};

use crate::base::{NodeProperties, Value, Array};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_column};

impl Expandable for proto::DpSum {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        // sum
        maximum_id += 1;
        let id_sum = maximum_id;
        computation_graph.insert(id_sum, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data")
                .ok_or_else(|| Error::from("data must be provided as an argument"))?],
            variant: Some(proto::component::Variant::Sum(proto::Sum {})),
            omit: true,
            batch: component.batch,
        });

        if self.mechanism.to_lowercase().as_str() == "simplegeometric" {
            let sum_max_id = *component.arguments.get("upper")
                .ok_or_else(|| Error::from("upper must be defined for geometric mechanism"))?;
            let sum_min_id = *component.arguments.get("lower")
                .ok_or_else(|| Error::from("lower must be defined for geometric mechanism"))?;

            // noising
            computation_graph.insert(component_id.clone(), proto::Component {
                arguments: hashmap![
                    "data".to_owned() => id_sum,
                    "lower".to_owned() => sum_min_id,
                    "upper".to_owned() => sum_max_id
                ],
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    enforce_constant_time: false,
                })),
                omit: false,
                batch: component.batch,
            });
        } else {

            // noising
            computation_graph.insert(component_id.clone(), proto::Component {
                arguments: hashmap![
                    "data".to_owned() => id_sum
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
        };

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_sum]
        })
    }
}

impl Report for proto::DpSum {
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

        let minimums = data_property.lower_f64()?;
        let maximums = data_property.upper_f64()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPSum".to_string(),
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
