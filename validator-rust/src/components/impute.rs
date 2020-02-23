use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints};

use crate::{base, components};
use crate::proto;
use crate::hashmap;
use crate::components::Component;
use ndarray::Array;

impl Component for proto::Impute {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let mut data_constraint = constraints.get("data").unwrap().clone();
        data_constraint.nullity = false;

        Ok(data_constraint)
    }

    fn is_valid(
        &self,
        constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        // check these properties are Some
        if constraint_utils::get_min_f64(constraints, "data").is_err()
            || constraint_utils::get_min_f64(constraints, "data").is_err()
            || constraint_utils::get_num_records(constraints, "data").is_err() {
            return false;
        }

        // all checks have passed
        true
    }

    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &constraint_utils::NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        Ok((maximum_id, HashMap::new()))
    }

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>> {
        None
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint_utils::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &constraint_utils::NodeConstraints,
    ) -> Option<f64> {
        None
    }

    fn summarize(
        &self,
        constraints: &NodeConstraints,
    ) -> Option<String> {
        Some("".to_string())
    }

    fn get_names(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        Ok(vec!["".to_string()])
    }
}