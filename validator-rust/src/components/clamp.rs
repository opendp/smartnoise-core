use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints, Nature, NatureContinuous, get_min_f64, get_num_records};


use crate::proto;

use crate::components::{Component};

use crate::utilities::serial::{Vector1DNull, Value};
use itertools::Itertools;


impl Component for proto::Clamp {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let mut data_constraint = constraints.get("data").unwrap().clone();

        data_constraint.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(get_min_f64(constraints, "data")?.iter()
                .zip(get_min_f64(constraints, "min")?)
                .zip(get_min_f64(constraints, "max")?)
                .map(|((d, min), max)| vec![d, &min, &max]
                    .iter().filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .fold1(|l, r| l.min(r)))
                .collect()),
            max: Vector1DNull::F64(get_min_f64(constraints, "data")?.iter()
                .zip(get_min_f64(constraints, "min")?)
                .zip(get_min_f64(constraints, "max")?)
                .map(|((d, min), max)| vec![d, &min, &max]
                    .iter().filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .fold1(|l, r| l.max(r)))
                .collect()),
        }));

        Ok(data_constraint)
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        if constraints.contains_key("data") &&
            ((constraints.contains_key("min") && constraints.contains_key("max")) ||
                constraints.contains_key("categories")) {
            return true
        }
        false
    }
}
