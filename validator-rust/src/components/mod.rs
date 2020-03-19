//! Component trait implementations
//!
//! Each component represents an abstract computation.
//! Example components are Materialize for loading a dataframe, Index for retrieving specific columns from the dataframe, Mean for aggregating data, LaplaceMechanism for privatizing data, etc.
//!
//! There are a set of possible behaviours each component may implement. Each behavior corresponds to a trait. A listing of traits is at the bottom of the page.
//!
//! Implementations of the traits are distributed among the module files.

use crate::errors::*;


mod transforms;
mod bin;
mod cast;
mod clamp;
mod count;
mod covariance;
mod dp_count;
mod dp_variance;
mod dp_covariance;
mod dp_histogram;
mod dp_maximum;
mod dp_median;
mod dp_minimum;
mod dp_mean;
mod dp_moment_raw;
mod dp_sum;
mod filter;
mod impute;
mod index;
mod kth_raw_sample_moment;
mod maximum;
mod materialize;
mod minimum;
mod partition;
mod quantile;
mod mean;
mod mechanism_exponential;
mod mechanism_gaussian;
mod mechanism_laplace;
mod mechanism_simple_geometric;
mod resize;
mod row_wise_min;
mod sum;
mod variance;

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
    /// Sufficient information to patch the runtime with more granular steps.
    /// More documentation at [ComponentExpansion](proto::ComponentExpansion).
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<proto::ComponentExpansion>;
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
            Bin, Cast, Clamp, Count, Covariance,

            DpCount, DpCovariance, DpHistogram, DpMaximum, DpMean, DpMedian, DpMinimum,
            DpMomentRaw, DpSum, DpVariance,

            Filter, Impute, Index, KthRawSampleMoment, Materialize, Maximum, Mean,

            ExponentialMechanism, GaussianMechanism, LaplaceMechanism, SimpleGeometricMechanism,

            Minimum, Quantile, Resize, Sum, Variance,

            Add, Subtract, Divide, Multiply, Power, Log, Modulo, Remainder, LogicalAnd, LogicalOr, Negate,
            Equal, LessThan, GreaterThan, Negative
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
    ) -> Result<proto::ComponentExpansion> {
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
            Clamp, DpCount, DpCovariance, DpHistogram, DpMaximum, DpMean, DpMedian, DpMinimum,
            DpMomentRaw, DpSum, DpVariance, Impute, ExponentialMechanism, GaussianMechanism,
            LaplaceMechanism, SimpleGeometricMechanism, Resize
        );

        // no expansion

        Ok(proto::ComponentExpansion {
            computation_graph: HashMap::new(),
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: Vec::new()
        })
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
            Count, Covariance, KthRawSampleMoment, Maximum, Mean, Minimum, Quantile, Sum, Variance
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
            DpMean
        );

        Ok(None)
    }
}