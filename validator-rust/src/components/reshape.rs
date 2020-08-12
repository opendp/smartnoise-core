use crate::errors::*;

use crate::components::Component;
use crate::base::{Value, ValueProperties, IndexKey, PartitionsProperties};
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

        let partition_count = match data_property.num_records {
            Some(0) | None => return Err("number of records must be one or more".into()),
            Some(1) => None,
            Some(x) => Some(x)
        };

        data_property.num_records = match self.shape.len() {
            // target is 0-d, so there will be one record in output
            0 => Some(1),
            // target is 1-d or 2-d, so there will be as many rows as the value in the first axis
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

        let matrix_properties = ValueProperties::Array(data_property);


        // multi-row inputs are reshaped to partitional outputs, one matrix per partition
        Ok(match partition_count {
            Some(x) => ValueProperties::Partitions(PartitionsProperties {
                children: (0..x)
                    .map(|idx| (idx.into(), matrix_properties.clone()))
                    .collect::<IndexMap<IndexKey, ValueProperties>>()
            }),
            None => matrix_properties
        }.into())
    }
}