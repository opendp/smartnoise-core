use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties, prepend};
use crate::utilities::json::{JSONRelease, value_to_json, AlgorithmInfo, privacy_usage_to_json};


impl Component for proto::DpCovariance {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPCovariance is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::DpCovariance {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // covariance
        current_id += 1;
        let id_covariance = current_id.clone();
        graph_expansion.insert(id_covariance, proto::Component {
            arguments: hashmap![
                "left".to_owned() => *component.arguments.get("left").unwrap(),
                "right".to_owned() => *component.arguments.get("right").unwrap()
            ],
            variant: Some(proto::component::Variant::from(proto::Covariance {})),
            omit: true,
            batch: component.batch,
        });

        // noise
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_covariance],
            variant: Some(proto::component::Variant::from(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Accuracy for proto::DpCovariance {
    fn accuracy_to_privacy_usage(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _properties: &base::NodeProperties,
        _accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _property: &base::NodeProperties,
    ) -> Option<f64> {
        None
    }
}

impl Report for proto::DpCovariance {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>> {

        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.get_min_f64().unwrap();
        let maximums = data_property.get_max_f64().unwrap();
        let num_records = data_property.get_num_records().unwrap();

        for column_number in 0..data_property.num_columns.unwrap() {

            let mut releaseInfo = HashMap::new();
            releaseInfo.insert("mechanism".to_string(), serde_json::json!(self.implementation.clone()));
            releaseInfo.insert("releaseValue".to_string(), value_to_json(&release).unwrap());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPCovariance".to_string(),
                variables: vec![],
                releaseInfo,
                privacyLoss: privacy_usage_to_json(&self.privacy_usage[column_number as usize].clone()),
                accuracy: None,
                batch: component.batch as u64,
                nodeID: node_id.clone() as u64,
                postprocess: false,
                algorithmInfo: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    argument: serde_json::json!({
                        "n": num_records,
                        "constraint": {
                            "lowerbound": minimums[column_number as usize],
                            "upperbound": maximums[column_number as usize]
                        }
                    })
                }
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
