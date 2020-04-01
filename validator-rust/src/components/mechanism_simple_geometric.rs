use crate::errors::*;


use std::collections::HashMap;


use crate::components::{Aggregator};
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, NodeProperties, SensitivitySpace, ValueProperties};
use crate::utilities::{prepend, expand_mechanism};


impl Component for proto::SimpleGeometricMechanism {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator.clone()
            .ok_or::<Error>("aggregator: missing".into())?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        data_property.aggregator = None;
        data_property.releasable = true;
        Ok(data_property.into())
    }

    fn get_names(
        &self,
        arg_vars: HashMap<String, Vec<String>>,
    ) -> Result<Vec<String>> {
        return Ok(arg_vars.values().cloned().flatten().collect::<Vec<String>>());
    }
}


impl Expandable for proto::SimpleGeometricMechanism {
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        expand_mechanism(
            &SensitivitySpace::KNorm(1),
            privacy_definition,
            component,
            properties,
            component_id,
            maximum_id
        )
    }
}
