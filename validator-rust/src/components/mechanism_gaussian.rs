use crate::errors::*;


use std::collections::HashMap;


use crate::components::Aggregator;
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, Properties, NodeProperties, ArrayND, get_constant};
use ndarray::Array;

impl Component for proto::GaussianMechanism {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data must be passed to GaussianMechanism")?.clone();

        data_property.aggregator.clone()
            .ok_or::<Error>("aggregator must be defined to run GaussianMechanism".into())?;

        // check that all properties are satisfied
        data_property.get_n()?;
        data_property.get_min_f64()?;
        data_property.get_max_f64()?;
        data_property.assert_non_null()?;

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


impl Expandable for proto::GaussianMechanism {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        input_properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // TODO: SECURITY: a user must not be able to define this directly
        if !input_properties.contains_key("sensitivity") {
            // sensitivity literal
            let aggregator = input_properties.get("data").unwrap().aggregator.clone()
                .ok_or::<Error>("aggregator must be defined to run GaussianMechanism".into())?;
            let sensitivity = Value::ArrayND(ArrayND::F64(Array::from(aggregator.component
                .compute_sensitivity(privacy_definition, &aggregator.properties)
                .unwrap()).into_dyn()));
            current_id += 1;
            let id_sensitivity = current_id.clone();
            graph_expansion.insert(id_sensitivity, get_constant(&sensitivity, &component.batch));

            // noising
            let mut noise_component = component.clone();
            noise_component.arguments.insert("sensitivity".to_string(), id_sensitivity);
            graph_expansion.insert(component_id, noise_component);
        }

        Ok((current_id, graph_expansion))
    }
}
