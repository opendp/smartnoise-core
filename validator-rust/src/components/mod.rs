use std::collections::HashMap;
use crate::utilities::constraint;
use crate::utilities::constraint::Constraint;
use crate::proto;

pub mod add;
pub mod row_wise_min;
pub mod dp_mean;


pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> Constraint;

    fn is_valid(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> bool;

    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: u32,
        component_id: u32,
        constraints: &constraint::NodeConstraints,
    ) -> (u32, HashMap<u32, proto::Component>);

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint,
    ) -> Option<f64>;

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage>;

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
    ) -> Option<f64>;

    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> String;
}



impl Component for proto::component::Value {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> Constraint {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.propagate_constraint(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn is_valid(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> bool {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.is_valid(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: u32,
        component_id: u32,
        constraints: &constraint::NodeConstraints,
    ) -> (u32, HashMap<u32, proto::Component>) {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.expand_graph(
                privacy_definition,
                component,
                maximum_id,
                component_id,
                constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint,
    ) -> Option<f64> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.compute_sensitivity(
                privacy_definition,
                constraint),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.accuracy_to_privacy_usage(
                privacy_definition,
                constraints,
                accuracy),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
    ) -> Option<f64> {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.privacy_usage_to_accuracy(
                privacy_definition,
                constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }

    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> String {
        use proto::component::Value;
        match self {
            // TODO: write a macro for delegating enum variants
            Value::Rowmin(x) => x.summarize(constraints),
            _ => panic!("a proto component is missing its Component trait")
        }
    }
}