use crate::errors::*;


use std::collections::HashMap;


use crate::components::Aggregator;
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, NodeProperties, ArrayND, get_literal, Sensitivity, prepend, ValueProperties};
use ndarray::Array;

impl Component for proto::ExponentialMechanism {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator.clone()
            .ok_or::<Error>("aggregator: missing".into())?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &Sensitivity::Exponential)?;

        data_property.releasable = true;
        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::ExponentialMechanism {
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<proto::ComponentExpansion> {
        expand_mechanism(
            &Sensitivity::Exponential,
            privacy_definition,
            component,
            properties,
            component_id,
            maximum_id
        )
    }
}



pub fn expand_mechanism(
    sensitivity_type: &Sensitivity,
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    properties: &base::NodeProperties,
    component_id: u32,
    maximum_id: u32,
) -> Result<proto::ComponentExpansion> {
    let mut current_id = maximum_id.clone();
    let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

    // always overwrite sensitivity. This is not something a user may configure
    let mut data_property = properties.get("data")
        .ok_or("data: missing")?.get_arraynd()
        .map_err(prepend("data:"))?.clone();

    let aggregator = data_property.aggregator.clone()
        .ok_or::<Error>("aggregator: missing".into())?;

    let sensitivity = Value::ArrayND(ArrayND::F64(Array::from(aggregator.component
        .compute_sensitivity(privacy_definition,
                             &aggregator.properties,
                             &sensitivity_type)?).into_dyn()));

    current_id += 1;
    let id_sensitivity = current_id.clone();
    let (patch_node, release) = get_literal(&sensitivity, &component.batch)?;
    computation_graph.insert(id_sensitivity.clone(), patch_node);
    releases.insert(id_sensitivity.clone(), release);

    // noising
    let mut noise_component = component.clone();
    noise_component.arguments.insert("sensitivity".to_string(), id_sensitivity);
    computation_graph.insert(component_id, noise_component);

    Ok(proto::ComponentExpansion {
        computation_graph,
        properties: HashMap::new(),
        releases,
        traversal: Vec::new()
    })
}
