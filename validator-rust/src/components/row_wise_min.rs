use std::collections::HashMap;
use crate::utilities::constraint::{Constraint, NodeConstraints, get_constraint};


use crate::proto;

use crate::components::Component;


use crate::utilities::serial::Value;


impl Component for proto::RowMin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &NodeConstraints,
    ) -> Result<Constraint, String> {
        Ok(get_constraint(constraints, "left")?.to_owned())
//        Ok(Constraint {
//            nullity: false,
//            releasable: false,
//            nature: None,
//            num_records: None
//        })
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _constraints: &NodeConstraints,
    ) -> Result<(), String> {
        // TODO: finish implementation
        Ok(())
    }

    fn get_names(
        &self,
        _constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}