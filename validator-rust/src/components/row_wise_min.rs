use std::collections::HashMap;
use crate::base::{Properties, NodeProperties, get_properties, Value};


use crate::proto;

use crate::components::Component;


impl Component for proto::RowMin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<Properties, String> {
        Ok(get_properties(properties, "left")?.to_owned())
//        Ok(property {
//            nullity: false,
//            releasable: false,
//            nature: None,
//            num_records: None
//        })
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _properties: &NodeProperties,
    ) -> Result<(), String> {
        // TODO: finish implementation
        Ok(())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}