use crate::errors::*;

use crate::components::Component;
use std::collections::HashMap;
use crate::base::{Value, AggregatorProperties, prepend, ValueProperties};
use crate::base;
use crate::proto;
use crate::components::transforms::propagate_binary_shape;

impl Component for proto::Filter {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let mut mask_property = properties.get("mask")
            .ok_or("mask: missing")?.get_arraynd()
            .map_err(prepend("mask:"))?.clone();

        propagate_binary_shape(&data_property, &mask_property)?;

        data_property.assert_is_not_aggregated()?;
        data_property.num_records = None;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &base::NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}