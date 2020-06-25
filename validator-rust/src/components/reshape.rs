use crate::errors::*;

use crate::components::Component;
use crate::base::{Value, ValueProperties, IndexKey};
use crate::{base, Warnable};
use crate::proto;
use crate::utilities::prepend;
use indexmap::map::IndexMap;


impl Component for proto::Reshape {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_releasable()?;

        data_property.num_records = match self.shape.len() {
            0 => Some(1),
            1 | 2 => Some(self.shape[0] as i64),
            _ => return Err("dimensionality may not be greater than 2".into())
        };

        data_property.num_columns = match self.shape.len() {
            0 | 1 => Some(1),
            2 => Some(self.shape[1] as i64),
            _ => return Err("dimensionality may not be greater than 2".into())
        };

        if data_property.num_records.unwrap() < 1 {
            return Err("number of records must be greater than zero".into())
        }
        if data_property.num_columns.unwrap() < 1 {
            return Err("number of columns must be greater than zero".into())
        }

        // Treat this as a new dataset, because number of rows is not necessarily the same anymore
        // This exists to prevent binary ops on non-conformable arrays from being approved
        data_property.dataset_id = Some(node_id as i64);

        Ok(ValueProperties::Array(data_property).into())
    }


}