use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};


use crate::base::{NodeProperties, Value};
use crate::utilities::json::{JSONRelease, value_to_json, AlgorithmInfo, privacy_usage_to_json};
use std::convert::TryFrom;
use crate::utilities::prepend;


impl Expandable for proto::DpCovariance {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        let arguments;
        let shape;
        let symmetric;
        match properties.get("data") {
            Some(data_property) => {
                let data_property = data_property.array()
                    .map_err(prepend("data:"))?.clone();

                let num_columns = data_property.num_columns()?;
                shape = vec![u32::try_from(num_columns)?, u32::try_from(num_columns)?];
                arguments = hashmap![
                    "data".to_owned() => *component.arguments.get("data")
                        .ok_or_else(|| Error::from("data must be provided as an argument"))?
                ];
                symmetric = true;
            },
            None => {
                let left_property = properties.get("left")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();
                let right_property = properties.get("right")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                shape = vec![u32::try_from(left_property.num_columns()?)?, u32::try_from(right_property.num_columns()?)?];
                arguments = hashmap![
                    "left".to_owned() => *component.arguments.get("left")
                        .ok_or_else(|| Error::from("left must be provided as an argument"))?,
                    "right".to_owned() => *component.arguments.get("right")
                        .ok_or_else(|| Error::from("right must be provided as an argument"))?
                ];
                symmetric = false;
            }
        };

        // covariance
        current_id += 1;
        let id_covariance = current_id;
        computation_graph.insert(id_covariance, proto::Component {
            arguments,
            variant: Some(proto::component::Variant::Covariance(proto::Covariance {
                finite_sample_correction: self.finite_sample_correction
            })),
            omit: true,
            submission: component.submission,
        });

        // noise
        current_id += 1;
        let id_noise = current_id;
        computation_graph.insert(id_noise, proto::Component {
            arguments: hashmap!["data".to_owned() => id_covariance],
            variant: Some(match self.mechanism.to_lowercase().as_str() {
                "laplace" => proto::component::Variant::LaplaceMechanism(proto::LaplaceMechanism {
                    privacy_usage: self.privacy_usage.clone()
                }),
                "gaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                    privacy_usage: self.privacy_usage.clone()
                }),
                _x => panic!("Unexpected invalid token {:?}", self.mechanism.as_str()),
            }),
            omit: true,
            submission: component.submission,
        });

        // reshape into matrix
        computation_graph.insert(component_id.clone(), proto::Component {
            arguments: hashmap!["data".to_owned() => id_noise],
            variant: Some(proto::component::Variant::Reshape(proto::Reshape {
                symmetric,
                layout: "row".to_string(),
                shape
            })),
            omit: false,
            submission: component.submission
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_covariance, id_noise],
            warnings: vec![]
        })
    }
}

impl Report for proto::DpCovariance {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<String>>,
    ) -> Result<Option<Vec<JSONRelease>>> {

        let argument;
        let statistic;

        if properties.contains_key("data") {
            let data_property = properties.get("data")
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();

            statistic = "DPCovariance".to_string();
            argument = serde_json::json!({
                "n": data_property.num_records()?,
                "constraint": {
                    "lowerbound": data_property.lower_f64()?,
                    "upperbound": data_property.upper_f64()?
                }
            });
        }
        else {
            let left_property = properties.get("left")
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();
            let right_property = properties.get("right")
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();

            statistic = "DPCrossCovariance".to_string();
            argument = serde_json::json!({
                "n": left_property.num_records()?,
                "constraint": {
                    "lowerbound_left": left_property.lower_f64()?,
                    "upperbound_left": left_property.upper_f64()?,
                    "lowerbound_right": right_property.lower_f64()?,
                    "upperbound_right": right_property.upper_f64()?
                }
            });
        }

        let privacy_usage: Vec<serde_json::Value> = self.privacy_usage.iter()
            .map(privacy_usage_to_json).clone().collect();


        Ok(Some(vec![JSONRelease {
            description: "DP release information".to_string(),
            statistic,
            variables: serde_json::json!(variable_names.cloned().unwrap_or_else(Vec::new).clone()),
            release_info: value_to_json(&release)?,
            privacy_loss: serde_json::json![privacy_usage],
            accuracy: None,
            submission: component.submission as u64,
            node_id: *node_id as u64,
            postprocess: false,
            algorithm_info: AlgorithmInfo {
                name: "".to_string(),
                cite: "".to_string(),
                mechanism: self.mechanism.clone(),
                argument
            }
        }]))
    }
}
