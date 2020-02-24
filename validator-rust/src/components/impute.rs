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

impl Component for proto::Impute {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let mut data_constraint = constraints.get("data").unwrap().clone();

        data_constraint.nullity = false;
        data_constraint.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(get_min_f64(constraints, "data")?.iter()
                .zip(get_min_f64(constraints, "min")?)
                .zip(get_min_f64(constraints, "max")?)
                .map(|((d, min), max)| {
                    match d {
                        Some(x) => vec![d, &min, &max]
                            .iter().filter(|x| x.is_some())
                            .map(|x| x.unwrap().clone())
                            .fold1(|l, r| l.min(r)),
                        // since there was no prior bound, nothing is known about the min
                        None => None
                    }
                })
                .collect()),
            max: Vector1DNull::F64(get_min_f64(constraints, "data")?.iter()
                .zip(get_min_f64(constraints, "min")?)
                .zip(get_min_f64(constraints, "max")?)
                .map(|((d, min), max)| {
                    match d {
                        // if there was a prior bound
                        Some(x) => vec![d, &min, &max]
                            .iter().filter(|x| x.is_some())
                            .map(|x| x.unwrap().clone())
                            .fold1(|l, r| l.max(r)),
                        // since there was no prior bound, nothing is known about the max
                        None => None
                    }
                })
                .collect()),
        }));

        Ok(data_constraint)
    }

    fn is_valid(
        &self,
        public_arguments: &HashMap<String, Value>,
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
