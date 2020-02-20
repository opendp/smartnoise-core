use std::collections::HashMap;
use crate::utilities::constraint::Constraint;

use crate::base;
use crate::proto;
use crate::hashmap;
use crate::components::Component;
use crate::utilities::constraint;

impl Component for proto::RowMin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> Constraint {
        Constraint {
            nullity: false,
            releasable: false,
            nature: None,
            num_records: None
        }
    }

    fn is_valid(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> bool {
        false
    }

    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: u32,
        component_id: u32,
        constraints: &constraint::NodeConstraints,
    ) -> (u32, HashMap<u32, proto::Component>) {
        (maximum_id, hashmap![component_id => component.to_owned()])
    }

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint,
    ) -> Option<f64> {
        None
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &constraint::NodeConstraints,
    ) -> Option<f64> {
        None
    }

    fn summarize(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> String {
        "".to_string()
    }
}