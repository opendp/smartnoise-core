use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{Properties, NodeProperties, Value};
use crate::utilities::json::{JSONRelease};



impl Component for proto::DpMomentRaw {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data argument missing from DPMomentRaw")?.clone();

        // check that all properties are satisfied
        data_property.get_n()?;
        data_property.get_min_f64()?;
        data_property.get_max_f64()?;
        data_property.assert_non_null()?;

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

impl Expandable for proto::DpMomentRaw {
    fn expand_graph(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // kth raw moment
        current_id += 1;
        let id_moment = current_id.clone();
        graph_expansion.insert(id_moment, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            variant: Some(proto::component::Variant::from(proto::KthRawSampleMoment {
                k: self.order
            })),
            omit: true,
            batch: component.batch,
        });

        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_moment],
            variant: Some(proto::component::Variant::from(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Accuracy for proto::DpMomentRaw {
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

impl Report for proto::DpMomentRaw {
    fn summarize(
        &self,
        _node_id: &u32,
        _component: &proto::Component,
        _properties: &NodeProperties,
        _release: &Value
    ) -> Option<Vec<JSONRelease>> {
        None
    }
}
