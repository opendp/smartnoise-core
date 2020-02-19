use std::collections::HashMap;
use crate::base::Constraint as Constraint;
use crate::proto;
use crate::hashmap;

pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> Constraint<T>;

    fn is_valid<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> bool;

    // return a hashmap of an expanded subgraph
    fn expand<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: i32,
        component_id: i32,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> (i32, HashMap<i32, proto::Component>);

    // compute the sensitivity
    fn sensitivity<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint<T>,
    ) -> Option<f64>;

    fn accuracy_to_privacy_usage<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint<T>,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage>;

    fn privacy_usage_to_accuracy<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> Option<f64>;

    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize<T>(
        &self,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> String;
}

impl Component for proto::RowMin {
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

    fn expand<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: i32,
        component_id: i32,
        constraints: &HashMap<String, Constraint<T>>,
    ) -> (i32, HashMap<i32, proto::Component>) {
        (maximum_id, hashmap![component_id => component.to_owned()])
    }

    fn sensitivity<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint<T>,
    ) -> Option<f64> {
        None
    }

    fn accuracy_to_privacy_usage<T>(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint<T>,
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