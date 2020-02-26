use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints};


use crate::proto;

use crate::components::{Component};

use crate::utilities::serial::{Value};



impl Component for proto::DataSource {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        Ok(Constraint {
            nullity: true,
            releasable: false,
            nature: None,
            c_stability: vec![1.],
            num_columns: Some(1),
            num_records: vec![None]
        })
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _constraints: &constraint_utils::NodeConstraints,
    ) -> Result<(), String> {
        Ok(())
    }

    fn get_names(
        &self,
        _constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}
