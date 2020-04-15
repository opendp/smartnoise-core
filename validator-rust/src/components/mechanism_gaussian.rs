use crate::errors::*;


use std::collections::HashMap;


use crate::components::{Aggregator};
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::{prepend, expand_mechanism};


impl Component for proto::GaussianMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into())
        }
        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(2))?;

        data_property.aggregator = None;
        data_property.releasable = true;

        Ok(data_property.into())
    }


}


impl Expandable for proto::GaussianMechanism {
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        expand_mechanism(
            &SensitivitySpace::KNorm(2),
            privacy_definition,
            component,
            properties,
            component_id,
            maximum_id
        )
    }
}
