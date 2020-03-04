use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;



use crate::{proto, base};

use crate::components::{Component};
use crate::base::{Value, Properties, NodeProperties};

// TODO: more checks needed here

impl Component for proto::Mean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data must be passed to Mean")?.clone();

        data_property.num_records = data_property.num_records.iter().map(|v| Some(1)).collect();

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
