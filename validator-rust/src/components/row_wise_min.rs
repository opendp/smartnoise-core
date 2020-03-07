use crate::errors::*;


use std::collections::HashMap;
use crate::base::{Properties, NodeProperties, Value};


use crate::proto;

use crate::components::Component;


impl Component for proto::RowMin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<Properties> {
        let left_prop = properties.get("left")
            .ok_or::<Error>("left is missing from row_wise_min".into())?.clone();

        Ok(left_prop)
//        Ok(property {
//            nullity: false,
//            releasable: false,
//            nature: None,
//            num_records: None
//        })
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}