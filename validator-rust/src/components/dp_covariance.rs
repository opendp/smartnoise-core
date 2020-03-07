use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Expandable, Report};


use crate::base::{Properties, NodeProperties, Value};
use crate::utilities::json::JSONRelease;


impl Component for proto::DpCovariance {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut left_property = properties.get("left")
            .ok_or("left argument missing from DPCovariance")?.clone();

        // check that all properties are satisfied
        println!("covariance left");
        let left_n = left_property.get_n()?;
        left_property.get_min_f64()?;
        left_property.get_max_f64()?;
        left_property.assert_non_null()?;

        let right_property = properties.get("right")
            .ok_or("right argument missing from DPCovariance")?.clone();

        // check that all properties are satisfied
        println!("covariance right");
        let right_n = right_property.get_n()?;
        right_property.get_min_f64()?;
        right_property.get_max_f64()?;
        right_property.assert_non_null()?;

        if !left_n.iter().zip(right_n).all(|(left, right)| left == &right) {
            return Err("n for left and right must be equivalent".into());
        }

        // TODO: derive proper propagation of covariance property
        left_property.num_records = (0..left_property.num_columns.unwrap()).map(|_| Some(1)).collect();
        left_property.releasable = true;

        Ok(left_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::DpCovariance {
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
        _node_id: &u32,
        _component: &proto::Component,
        _properties: &NodeProperties,
        _release: &Value,
    ) -> Option<Vec<JSONRelease>> {
        None
    }
}
