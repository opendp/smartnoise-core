use crate::errors::*;


pub mod transforms;
pub mod bin;
pub mod cast;
pub mod clamp;
pub mod constant;
pub mod count;
pub mod covariance;
pub mod dp_count;
pub mod dp_variance;
pub mod dp_covariance;
pub mod dp_histogram;
pub mod dp_maximum;
pub mod dp_median;
pub mod dp_minimum;
pub mod dp_mean;
pub mod dp_moment_raw;
pub mod dp_sum;
pub mod filter;
pub mod impute;
pub mod index;
pub mod kth_raw_sample_moment;
pub mod maximum;
pub mod materialize;
pub mod minimum;
pub mod partition;
pub mod quantile;
pub mod mean;
pub mod mechanism_exponential;
pub mod mechanism_gaussian;
pub mod mechanism_laplace;
pub mod mechanism_simple_geometric;
pub mod resize;
pub mod row_wise_min;
pub mod sum;
pub mod variance;

use std::collections::HashMap;

use crate::base::{Value, NodeProperties, Sensitivity, ValueProperties};
use crate::proto;
use crate::utilities::json::{JSONRelease};
use crate::hashmap;


/// Universal Component trait
///
/// To be a component, a struct must represent an abstract computation, for which properties can be derived about the resulting data.
pub trait Component {
    /// Given properties known about private arguments, and public arguments, derive properties about the resulting data.
    ///
    /// A component must fail to propagate properties if requirements on the input properties are not met.
    /// For example, if a Component represents an abstract computation that requires prior knowledge of the number of records to be safe or function properly,
    /// the propagate_property implementation is expected to return an error state if the prior knowledge is not known.
    ///
    /// For example, if a definition of privacy is used that is incompatible with the abstract computation,
    /// the propagate_property implementation is expected to return an error state.
    ///
    /// # Arguments
    /// * `self` - the protobuf object corresponding to the prost protobuf struct
    /// * `privacy_definition` - the definition of privacy under which the computation takes place
    /// * `public_arguments` - actual data values of arguments, typically either supplied literals or released values.
    /// * `properties` - derived properties of private input arguments
    ///
    /// # Returns
    /// Derived properties on the data resulting from the abstract computation
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties>;

    /// Utility function for a recursive algorithm to derive human readable names on the columns in the output data.
    fn get_names(
        &self,
        properties: &NodeProperties,
    ) -> Result<Vec<String>>;
}

/// Expandable Component trait
///
/// When a component is expandable, it represents a higher order computation that may be expressed in multiple components that are more granular.
/// Oftentimes Expandable components correspond to differentially private algorithms,
/// that are represented in terms of an aggregation and a mechanism.
pub trait Expandable {
    /// Concrete implementation for an Expandable component that returns a patch that may be applied to a computation graph.
    ///
    /// # Arguments
    /// * `self` - the protobuf object corresponding to the prost protobuf struct
    /// * `privacy_definition` - definition of privacy to use when expanding. Some expansions are not valid under some privacy definitions
    /// * `component` - contains additional metadata about the argument node ids
    /// * `properties` - properties on the data supplied as arguments
    /// * `component_id` - the id of the node to expand. The final node in the returned patch must use this id.
    /// * `maximum_id` - the starting id for which additional nodes may be added to the graph without overwriting existing nodes
    ///
    /// # Returns
    /// * `0` - the new maximum id, for which nodes may be added to the graph without overwriting existing nodes
    /// * `1` - the patch to be applied to the computation graph, such that the patched graph represents the expanded component
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)>;
}

/// Aggregator Component trait
///
/// When a component is an aggregator, the abstract computation the component represents combines multiple rows together into a single value.
/// For example, a mean, minimum, or scoring function on a dataset.
pub trait Aggregator {
    /// Derivation for the sensitivity of an aggregator based on available local metadata.
    ///
    /// The sensitivity is the maximum amount that a perturbation of input data may have on the resulting value.
    /// The type of perturbation is described in the privacy_definition.
    ///
    /// # Arguments
    /// * `self` - the protobuf object corresponding to the prost protobuf struct
    /// * `privacy_definition` - the definition of privacy under which the sensitivity is to be computed
    /// * `properties` - derived properties for the input data
    /// * `sensitivity_type` - space for which the sensitivity is computed within
    ///
    /// # Returns
    /// Sensitivities for each of the values in the resulting computation
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>>;
}

/// Accuracy component trait (not yet implemented)
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

/// Report component trait
///
/// Reportable components correspond to a computation that a researcher may want a JSON summary for
pub trait Report {
    /// Summarize the relevant metadata around a computation in a readable, JSON-serializable format.
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>>;
}



impl Component for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        macro_rules! propagate_property {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.propagate_property(privacy_definition, public_arguments, properties)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        propagate_property!(
            // INSERT COMPONENT LIST
            Bin, Cast, Clamp, Constant, Count, Covariance,

            Dpcount, Dpcovariance, Dphistogram, Dpmaximum, Dpmean, Dpmedian, Dpminimum,
            Dpmomentraw, Dpsum, Dpvariance,

            Filter, Impute, Index, Kthrawsamplemoment, Materialize, Maximum, Mean,

            Exponentialmechanism, Gaussianmechanism, Laplacemechanism, Simplegeometricmechanism,

            Minimum, Quantile, Resize, Rowmin, Sum, Variance,

            Add, Subtract, Divide, Multiply, Power, Log, Modulo, Remainder, And, Or, Negate,
            Equal, Lessthan, Greaterthan, Negative
        );

        return Err(format!("proto component {:?} is missing its Component trait", self).into())
    }

    fn get_names(
        &self,
        properties: &NodeProperties,
    ) -> Result<Vec<String>> {

        macro_rules! get_names{
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.get_names(properties)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        get_names!(
            // INSERT COMPONENT LIST
//            Rowmin, Dpmean, Impute
        );
        // TODO: default implementation

        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        macro_rules! expand_component {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.expand_component(privacy_definition, component, properties, component_id, maximum_id)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        expand_component!(
            // INSERT COMPONENT LIST
            Clamp, Dpcount, Dpcovariance, Dphistogram, Dpmaximum, Dpmean, Dpmedian, Dpminimum,
            Dpmomentraw, Dpsum, Dpvariance, Impute, Exponentialmechanism, Gaussianmechanism,
            Laplacemechanism, Simplegeometricmechanism, Resize
        );

        // no expansion
        return Ok((maximum_id, hashmap!()))
    }
}

impl Aggregator for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>> {
        macro_rules! compute_sensitivity {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.compute_sensitivity(privacy_definition, properties, sensitivity_type)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        compute_sensitivity!(
            // INSERT COMPONENT LIST
            Count, Covariance, Kthrawsamplemoment, Maximum, Mean, Minimum, Quantile, Sum, Variance
        );

        Err("sensitivity is not implemented".into())
    }
}

impl Accuracy for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        macro_rules! accuracy_to_privacy_usage {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.accuracy_to_privacy_usage(privacy_definition, properties, accuracy)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        accuracy_to_privacy_usage!(
            // INSERT COMPONENT LIST
//            Dpmean
        );

        None
    }

    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<f64> {
        macro_rules! privacy_usage_to_accuracy {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.privacy_usage_to_accuracy(privacy_definition, properties)
                                .chain_err(|| format!("node specification {:?}:", self))
                       }
                    )*
                }
            }
        }

        privacy_usage_to_accuracy!(
            // INSERT COMPONENT LIST
//            Dpmean
        );

        None
    }
}

impl Report for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>> {

        macro_rules! summarize{
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.summarize(node_id, component, public_arguments, properties, release)
                                .chain_err(|| format!("node specification: {:?}:", self))
                       }
                    )*
                }
            }
        }

        summarize!(
            // INSERT COMPONENT LIST
            Dpmean
        );

        Ok(None)
    }
}