use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};
use ndarray::{Array, arr1};
use crate::utilities::serial::serialize_value;
use crate::base::{Properties, NodeProperties, Value, get_constant, ArrayND};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};

use serde_json;

impl Component for proto::DpMaximum {
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data argument missing from DpMaximum")?.clone();

        public_arguments.get("candidates")
            .ok_or::<Error>("candidates must be defined to compute a DPMaximum".into())?;

        data_property.num_records = (0..data_property.num_columns.unwrap()).map(|_| Some(1)).collect();
        data_property.releasable = true;

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::DpMaximum {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // Maximum
        current_id += 1;
        let id_Maximum = current_id.clone();
        graph_expansion.insert(id_Maximum, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            variant: Some(proto::component::Variant::from(proto::Maximum {})),
            omit: true,
            batch: component.batch,
        });

        let id_candidates = component.arguments.get("candidates").unwrap().clone();

        // sanitizing
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_Maximum, "candidates".to_owned() => id_candidates.clone()],
            variant: Some(proto::component::Variant::from(proto::ExponentialMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Accuracy for proto::DpMaximum {
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

impl Report for proto::DpMaximum {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        properties: &NodeProperties,
        release: &Value
    ) -> Option<Vec<JSONRelease>> {
        None
    }
}
