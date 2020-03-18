use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease};



impl Component for proto::DpMedian {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPMedian is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::DpMedian {
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

        // median
        current_id += 1;
        let id_median = current_id.clone();
        graph_expansion.insert(id_median, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            variant: Some(proto::component::Variant::from(proto::Quantile {
                quantile: 0.5
            })),
            omit: true,
            batch: component.batch,
        });

        let id_candidates = component.arguments.get("candidates").unwrap().clone();

        // sanitizing
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_median, "candidates".to_owned() => id_candidates],
            variant: Some(proto::component::Variant::from(proto::ExponentialMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Accuracy for proto::DpMedian {
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

impl Report for proto::DpMedian {
    fn summarize(
        &self,
        _node_id: &u32,
        _component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        _properties: &NodeProperties,
        _release: &Value
    ) -> Result<Option<Vec<JSONRelease>>> {
        Ok(None)
    }
}
