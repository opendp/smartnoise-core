use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{
    Constraint, NodeConstraints, Nature, NatureContinuous, get_min_f64};

use crate::{base, components};
use crate::proto;
use crate::hashmap;
use crate::components::{Component, Expandable};
use ndarray::Array;
use crate::utilities::serial::{Vector1DNull, Value};
use itertools::Itertools;
use crate::utilities::buffer::NodeArguments;

impl Component for proto::DataSource {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
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
        public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        true
    }
}
