use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints, Nature, NatureContinuous, get_min_f64, get_num_records};

use crate::{base, components};
use crate::proto;
use crate::hashmap;
use crate::components::{Component, Expandable};
use ndarray::Array;
use crate::utilities::serial::{Vector1DNull, Value, ArrayND};
use itertools::Itertools;
use crate::utilities::buffer::NodeArguments;

impl Component for proto::Resize {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let mut data_constraint = constraints.get("data").unwrap().clone();

        // when resizing, nullity may become true to add additional rows
        data_constraint.nullity = true;
        data_constraint.num_records = match public_arguments.get("n").unwrap() {
            Value::ArrayND(array) => match array {
                ArrayND::I64(array) => match array.ndim() {
                    0 => (0..data_constraint.num_columns.unwrap())
                        .collect::<Vec<i64>>().iter().map(|x| Some(array.first().unwrap().clone())).collect(),
                    _ => return Err("n must be a scalar".to_string())
                }
                _ => return Err("n must be an integer".to_string())
            }
            _ => return Err("n must be packed inside an ArrayND".to_string())
        };

        Ok(data_constraint)
    }

    fn is_valid(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        public_arguments.contains_key("n")
    }
}
