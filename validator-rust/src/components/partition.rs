use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, prepend, ValueProperties};


impl Component for proto::Partition {
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

        let mut by_property = properties.get("by")
            .ok_or("by: missing")?.get_arraynd()
            .map_err(prepend("by:"))?.clone();

        let by_num_columns= by_property.num_columns
            .ok_or::<Error>("number of columns must be known on by".into())?;
        if by_num_columns != 1 {
            return Err("Partition's by argument must contain a single column".into());
        }

        data_property.assert_is_not_aggregated()?;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
