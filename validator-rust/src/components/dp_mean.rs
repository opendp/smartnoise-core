use std::collections::HashMap;
use crate::utilities::properties as property_utils;
use crate::utilities::properties::{Properties, NodeProperties, get_properties, get_literal};


use crate::proto;
use crate::hashmap;
use crate::components::{Component, Accuracy, Privatize, Expandable, Report};
use ndarray::Array;
use crate::utilities::serial::{Value, serialize_value, ArrayND};


impl Component for proto::DpMean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &property_utils::NodeProperties,
    ) -> Result<Properties, String> {
        Ok(get_properties(properties, "left")?.to_owned())

//        Ok(Properties {
//            nullity: false,
//            releasable: true,
//            nature: Some(property_utils::Nature::Continuous(property_utils::NatureContinuous {
//                min: property_utils::get_min(&properties, "data")?,
//                max: property_utils::get_max(&properties, "data")?,
//            })),
//            num_records: property_utils::get_num_records(&properties, "data")?,
//        })
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &property_utils::NodeProperties,
    ) -> Result<(), String> {
        let data_property = property_utils::get_properties(&properties, "data")?.clone();

        data_property.get_n()?;
        data_property.get_min_f64()?;
        data_property.get_max_f64()?;
        data_property.assert_non_null()?;

        Ok(())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}

impl Expandable for proto::DpMean {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &property_utils::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // mean
        current_id += 1;
        let id_mean = current_id.clone();
        graph_expansion.insert(id_mean, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            value: Some(proto::component::Value::Mean(proto::Mean {})),
            omit: true,
            batch: component.batch,
        });

        let sensitivity = Value::ArrayND(ArrayND::F64(Array::from(component.value.to_owned().unwrap()
                .compute_sensitivity(privacy_definition, properties)
                .unwrap()).into_dyn()));

        // sensitivity literal
        current_id += 1;
        let id_sensitivity = current_id.clone();
        graph_expansion.insert(id_sensitivity, get_literal(&sensitivity, &component.batch));

        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_mean, "sensitivity".to_owned() => id_sensitivity],
            value: Some(proto::component::Value::LaplaceMechanism(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: true,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Privatize for proto::DpMean {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<Vec<f64>> {
        let data_property = property_utils::get_properties(properties, "data").ok()?;
        let min = data_property.get_min_f64().ok()?;
        let max = data_property.get_max_f64().ok()?;
        let num_records = data_property.get_n().ok()?;

        Some(min
            .iter()
            .zip(max)
            .zip(num_records)
            .map(|((l, r), n)| (l - r) / n as f64)
            .collect())
    }
}

impl Accuracy for proto::DpMean {
    fn accuracy_to_privacy_usage(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _properties: &property_utils::NodeProperties,
        _accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _property: &property_utils::NodeProperties,
    ) -> Option<f64> {
        None
    }
}

impl Report for proto::DpMean {
    fn summarize(
        &self,
        _properties: &NodeProperties,
    ) -> Option<String> {
        None
    }
}