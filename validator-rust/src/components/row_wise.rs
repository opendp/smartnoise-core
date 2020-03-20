use crate::errors::*;


use std::collections::HashMap;
use crate::base::{NodeProperties, Value, prepend, ValueProperties, Nature};


use crate::proto;

use crate::components::Component;
use crate::components::transforms::propagate_binary_shape;


impl Component for proto::RowMin {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        // TODO: adjust bounds
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(left_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}