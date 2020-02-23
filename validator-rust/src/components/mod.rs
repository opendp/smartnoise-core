use std::collections::HashMap;
use crate::utilities::constraint;
use crate::utilities::constraint::{Constraint, NodeConstraints};
use crate::proto;

pub mod add;
pub mod row_wise_min;
pub mod dp_mean;
pub mod impute;


pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Constraint, String>;

    fn is_valid(
        &self,
        constraints: &NodeConstraints,
    ) -> bool;

    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String>;

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>>;

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage>;

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<f64>;

    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        constraints: &NodeConstraints,
    ) -> Option<String>;

    fn get_names(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String>;
}



impl Component for proto::component::Value {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Constraint, String> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.propagate_constraint(constraints),
            Value::Dpmean(x) => x.propagate_constraint(constraints),
            Value::Impute(x) => x.propagate_constraint(constraints),
            _ => Err("a proto component is missing its Component trait".to_string())
        }
    }

    fn is_valid(
        &self,
        constraints: &NodeConstraints,
    ) -> bool {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.is_valid(constraints),
            Value::Dpmean(x) => x.is_valid(constraints),
            Value::Impute(x) => x.is_valid(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.expand_graph(
                privacy_definition,
                component,
                constraints,
                component_id,
                maximum_id,
            ),
            Value::Dpmean(x) => x.expand_graph(
                privacy_definition,
                component,
                constraints,
                component_id,
                maximum_id,
            ),
            Value::Impute(x) => x.expand_graph(
                privacy_definition,
                component,
                constraints,
                component_id,
                maximum_id,
            ),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.compute_sensitivity(
                privacy_definition,
                constraints),
            Value::Dpmean(x) => x.compute_sensitivity(
                privacy_definition,
                constraints),
            Value::Impute(x) => x.compute_sensitivity(
                privacy_definition,
                constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.accuracy_to_privacy_usage(
                privacy_definition,
                constraints,
                accuracy),
            Value::Dpmean(x) => x.accuracy_to_privacy_usage(
                privacy_definition,
                constraints,
                accuracy),
            Value::Impute(x) => x.accuracy_to_privacy_usage(
                privacy_definition,
                constraints,
                accuracy),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<f64> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.privacy_usage_to_accuracy(
                privacy_definition,
                constraints),
            Value::Dpmean(x) => x.privacy_usage_to_accuracy(
                privacy_definition,
                constraints),
            Value::Impute(x) => x.privacy_usage_to_accuracy(
                privacy_definition,
                constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        constraints: &NodeConstraints,
    ) -> Option<String> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.summarize(constraints),
            Value::Dpmean(x) => x.summarize(constraints),
            Value::Impute(x) => x.summarize(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn get_names(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.get_names(constraints),
            Value::Dpmean(x) => x.get_names(constraints),
            Value::Impute(x) => x.get_names(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }
}
