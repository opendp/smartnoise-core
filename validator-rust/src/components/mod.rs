use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

pub mod cast;
pub mod clamp;
pub mod constant;
pub mod dp_mean;
pub mod impute;
pub mod index;
pub mod materialize;
pub mod mean;
pub mod mechanism_laplace;
pub mod resize;
pub mod row_wise_min;

use std::collections::HashMap;

use crate::base::{ArrayND, Value, Vector2DJagged, Nature, Vector1DNull, NatureContinuous, NatureCategorical, Properties, NodeProperties};
use crate::proto;

use crate::hashmap;

pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<Properties>;

    fn get_names(
        &self,
        properties: &NodeProperties,
    ) -> Result<Vec<String>>;
}

pub trait Expandable {
    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)>;
}

pub trait Privatize {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<Vec<f64>>;
}

pub trait Accuracy {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage>;

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<f64>;
}

pub trait Report {
    // for json construction. Return type should be a generic serializable struct, not a String
    fn summarize(
        &self,
        properties: &NodeProperties,
    ) -> Option<String>;
}



impl Component for proto::component::Value {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<Properties> {
        macro_rules! propagate_property {
            ($self:ident, $public_arguments: ident, $properties: ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.propagate_property($public_arguments, $properties)
                       }
                    )*
                }
            }
        }

        propagate_property!(self, public_arguments, properties,
            // INSERT COMPONENT LIST
            Rowmin, Dpmean, Impute, Resize, Clamp, Constant, Materialize, Cast, Index, LaplaceMechanism, Mean
        );

        println!("{:?}", self);

        return Err("a proto component is missing its Component trait".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {

        macro_rules! get_names{
            ($self:ident, $properties:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.get_names($properties)
                       }
                    )*
                }
            }
        }

        get_names!(self, properties,
            // INSERT COMPONENT LIST
//            Rowmin, Dpmean, Impute
        );
        // TODO: default implementation

        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::component::Value {
    // return a hashmap of an expanded subgraph
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        macro_rules! expand_graph {
            ($self:ident, $privacy_definition:ident, $component:ident, $properties:ident, $component_id:ident, $maximum_id:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.expand_graph($privacy_definition, $component, $properties, $component_id, $maximum_id)
                       }
                    )*
                }
            }
        }

        expand_graph!(self, privacy_definition, component, properties, component_id, maximum_id,
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
        properties: &NodeProperties,
    ) -> Option<Vec<f64>> {
        macro_rules! compute_sensitivity {
            ($self:ident, $privacy_definition:ident, $properties:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.compute_sensitivity($privacy_definition, $properties)
                       }
                    )*
                }
            }
        }

        compute_sensitivity!(self, privacy_definition, properties,
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
        properties: &NodeProperties,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        macro_rules! accuracy_to_privacy_usage {
            ($self:ident, $privacy_definition:ident, $properties:ident, $accuracy:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.accuracy_to_privacy_usage($privacy_definition, $properties, $accuracy)
                       }
                    )*
                }
            }
        }

        accuracy_to_privacy_usage!(self, privacy_definition, properties, accuracy,
            // INSERT COMPONENT LIST
            Dpmean
        );

        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<f64> {
        macro_rules! privacy_usage_to_accuracy {
            ($self:ident, $privacy_definition:ident, $properties:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.privacy_usage_to_accuracy($privacy_definition, $properties)
                       }
                    )*
                }
            }
        }

        privacy_usage_to_accuracy!(self, privacy_definition, properties,
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
        _properties: &NodeProperties,
    ) -> Option<String> {

        macro_rules! summarize{
            ($self:ident, $properties:ident, $( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Value::$variant(x) = $self {
                            return x.summarize($properties)
                       }
                    )*
                }
            }
        }

        summarize!(self, properties,
            // INSERT COMPONENT LIST
//            Rowmin, Dpmean, Impute
        );
        // TODO: default implementation

        None
    }
}