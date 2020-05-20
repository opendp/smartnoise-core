use crate::errors::*;

use crate::components::Component;
use std::collections::HashMap;
use crate::base::{Value, ValueProperties, DataType};
use crate::utilities::prepend;
use crate::{base, Warnable};
use crate::proto;
use crate::components::transforms::propagate_binary_shape;

impl Component for proto::Filter {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        let mask_property = properties.get("mask")
            .ok_or("mask: missing")?.array()
            .map_err(prepend("mask:"))?.clone();

        if !mask_property.releasable {
            mask_property.assert_is_not_aggregated()?;
        }

        if mask_property.data_type != DataType::Bool {
            return Err("mask: must be boolean".into())
        }

        if mask_property.num_columns()? != 1 {
            return Err("mask: number of columns must be one".into())
        }

        propagate_binary_shape(&data_property, &mask_property)?;

        // the number of records is not known after filtering rows
        data_property.num_records = None;

        // This exists to prevent binary ops on non-conformable arrays from being approved
        data_property.dataset_id = Some(node_id as i64);

        // no longer know if the data has a nonzero number of records
        data_property.is_not_empty = false;

        Ok(ValueProperties::Array(data_property).into())
    }
}