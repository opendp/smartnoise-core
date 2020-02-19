use std::collections::HashMap;
use crate::base::Constraint as Constraint;

use crate::base;
use crate::proto;
use crate::hashmap;

impl base::Component for proto::RowMin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> Constraint<T> {
        Constraint::<T> {
            nullity: false,
            is_releasable: false,
            min: None,
            max: None,
            categories: None,
            num_records: None
        }
    }

    fn is_valid<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> bool {
        false
    }

    fn expand_graph<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: i32,
        component_id: i32,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> (i32, HashMap<i32, proto::Component>) {
        (maximum_id, hashmap![component_id => component.to_owned()])
    }

    fn compute_sensitivity<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint<T>,
    ) -> Option<f64> {
        None
    }

    fn accuracy_to_privacy_usage<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &HashMap<String, Constraint<T>>,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &HashMap<String, Constraint<T>>,
    ) -> Option<f64> {
        None
    }

    fn summarize<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> String {
        "".to_string()
    }
}