pub mod row_wise_min;
pub mod dp_mean;
pub mod impute;
pub mod resize;
pub mod literal;
pub mod clamp;
pub mod datasource;

use std::collections::HashMap;

use crate::utilities::constraint::{Constraint, NodeConstraints};
use crate::proto;

use crate::hashmap;

use crate::utilities::serial::Value;

pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &NodeConstraints,
    ) -> Result<Constraint, String>;

    fn is_valid(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &NodeConstraints,
    ) -> Result<(), String>;

    fn get_names(
        &self,
        constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String>;
}

pub trait Expandable {
    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String>;
}

pub trait Privatize {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>>;
}

pub trait Accuracy {
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
}

pub trait Report {
    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        constraints: &NodeConstraints,
    ) -> Option<String>;
}



impl Component for proto::component::Value {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &NodeConstraints,
    ) -> Result<Constraint, String> {
        macro_rules! propagate_constraint {
            ($self:ident, $public_arguments: ident, $constraints: ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.propagate_constraint($public_arguments, $constraints)
                        }
                    )*
                }
            }
        }

        propagate_constraint!(self, public_arguments, constraints,
            // INSERT COMPONENT LIST
            Datasource, Rowmin, Dpmean, Impute, Literal, Resize, Clamp
        );

        return Err("a proto component is missing its Component trait".to_string())
    }

    fn is_valid(
        &self,
        public_arguments: &HashMap<String, Value>,
        constraints: &NodeConstraints,
    ) -> Result<(), String> {
        macro_rules! is_valid {
            ($self:ident, $public_arguments: ident, $constraints: ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.is_valid($public_arguments, $constraints)
                        }
                    )*
                }
            }
        }

        is_valid!(self, public_arguments, constraints,
            // INSERT COMPONENT LIST
            Datasource, Rowmin, Dpmean, Impute, Literal, Resize, Clamp
        );

        // an unknown component is not valid
        Err("a component is missing its validator implementation".to_string())
    }

    fn get_names(
        &self,
        _constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {

        macro_rules! get_names{
            ($self:ident, $constraints:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.get_names($constraints)
                        }
                    )*
                }
            }
        }

        get_names!(self, constraints,
            // INSERT COMPONENT LIST
//            Rowmin, Dpmean, Impute
        );
        // TODO: default implementation

        Err("get_names not implemented".to_string())
    }
}

impl Expandable for proto::component::Value {
    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        macro_rules! expand_graph {
            ($self:ident, $privacy_definition:ident, $component:ident, $constraints:ident, $component_id:ident, $maximum_id:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.expand_graph($privacy_definition, $component, $constraints, $component_id, $maximum_id)
                        }
                    )*
                }
            }
        }

        expand_graph!(self, privacy_definition, component, constraints, component_id, maximum_id,
            // INSERT COMPONENT LIST
            Dpmean, Clamp, Impute, Resize
        );

        // no expansion
        return Ok((maximum_id, hashmap!()))
    }
}

impl Privatize for proto::component::Value {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>> {
        macro_rules! compute_sensitivity {
            ($self:ident, $privacy_definition:ident, $constraints:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.compute_sensitivity($privacy_definition, $constraints)
                        }
                    )*
                }
            }
        }

        compute_sensitivity!(self, privacy_definition, constraints,
            // INSERT COMPONENT LIST
            Dpmean
        );

        None
    }
}

impl Accuracy for proto::component::Value {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        macro_rules! accuracy_to_privacy_usage {
            ($self:ident, $privacy_definition:ident, $constraints:ident, $accuracy:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.accuracy_to_privacy_usage($privacy_definition, $constraints, $accuracy)
                        }
                    )*
                }
            }
        }

        accuracy_to_privacy_usage!(self, privacy_definition, constraints, accuracy,
            // INSERT COMPONENT LIST
            Dpmean
        );

        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<f64> {
        macro_rules! privacy_usage_to_accuracy {
            ($self:ident, $privacy_definition:ident, $constraints:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.privacy_usage_to_accuracy($privacy_definition, $constraints)
                        }
                    )*
                }
            }
        }

        privacy_usage_to_accuracy!(self, privacy_definition, constraints,
            // INSERT COMPONENT LIST
            Dpmean
        );

        None
    }
}

impl Report for proto::component::Value {
    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        _constraints: &NodeConstraints,
    ) -> Option<String> {

        macro_rules! summarize{
            ($self:ident, $constraints:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.summarize($constraints)
                        }
                    )*
                }
            }
        }

        summarize!(self, constraints,
            // INSERT COMPONENT LIST
//            Rowmin, Dpmean, Impute
        );
        // TODO: default implementation

        None
    }
}
