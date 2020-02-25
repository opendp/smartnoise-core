use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{
    Constraint, Nature, NatureContinuous, get_min_f64};


use crate::proto;

use crate::components::{Component};

use crate::utilities::serial::{Vector1DNull, Value};
use itertools::Itertools;


impl Component for proto::Impute {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
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
                        Some(_x) => vec![d, &min, &max]
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
                        Some(_x) => vec![d, &min, &max]
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
