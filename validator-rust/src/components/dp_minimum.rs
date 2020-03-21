use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties, prepend};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};



impl Component for proto::DpMinimum {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPMinimum is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::DpMinimum {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = maximum_id.clone();
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        // minimum
        current_id += 1;
        let id_minimum = current_id.clone();
        computation_graph.insert(id_minimum, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            variant: Some(proto::component::Variant::from(proto::Minimum {})),
            omit: true,
            batch: component.batch,
        });

        let id_candidates = component.arguments.get("candidates").unwrap().clone();

        // sanitizing
        computation_graph.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_minimum, "candidates".to_owned() => id_candidates],
            variant: Some(proto::component::Variant::from(proto::ExponentialMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_minimum]
        })
    }
}

impl Accuracy for proto::DpMinimum {
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


impl Report for proto::DpMinimum {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.get_min_f64().unwrap();
        let maximums = data_property.get_max_f64().unwrap();

        for column_number in 0..data_property.num_columns.unwrap() {
            let mut release_info = HashMap::new();
            release_info.insert("mechanism".to_string(), serde_json::json!(self.implementation.clone()));
            release_info.insert("releaseValue".to_string(), value_to_json(&release).unwrap());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMinimum".to_string(),
                variables: vec![],
                release_info,
                privacy_loss: privacy_usage_to_json(&self.privacy_usage[column_number as usize].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: node_id.clone() as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    argument: serde_json::json!({
                        "constraint": {
                            "lowerbound": minimums[column_number as usize],
                            "upperbound": maximums[column_number as usize]
                        }
                    }),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
