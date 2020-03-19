use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{NodeProperties, Value, standardize_categorical_argument, Vector2DJagged, ValueProperties, prepend};
use crate::utilities::json::{JSONRelease, privacy_usage_to_json, AlgorithmInfo, value_to_json};


impl Component for proto::DpCount {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPCount is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::DpCount {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut maximum_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // count
        maximum_id += 1;
        let id_count = maximum_id.clone();
        graph_expansion.insert(id_count, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            variant: Some(proto::component::Variant::Count(proto::Count {})),
            omit: true,
            batch: component.batch,
        });

        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap![
                "data".to_owned() => id_count,
                "count_min".to_owned() => *component.arguments.get("count_min").unwrap(),
                "count_max".to_owned() => *component.arguments.get("count_max").unwrap()
            ],
            variant: Some(proto::component::Variant::from(proto::SimpleGeometricMechanism {
                privacy_usage: self.privacy_usage.clone(),
                enforce_constant_time: false
            })),
            omit: false,
            batch: component.batch,
        });

        Ok((maximum_id, graph_expansion))
    }
}

impl Accuracy for proto::DpCount {
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

impl Report for proto::DpCount {
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

        for column_number in 0..data_property.num_columns.unwrap() {

            let mut releaseInfo = HashMap::new();
            releaseInfo.insert("mechanism".to_string(), serde_json::json!(self.implementation.clone()));
            releaseInfo.insert("releaseValue".to_string(), value_to_json(&release).unwrap());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPCount".to_string(),
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
                    argument: serde_json::json!({})
                }
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
