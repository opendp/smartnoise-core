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

use crate::base::{Value, NodeProperties, SensitivityType, ValueProperties, Sensitivity, ArrayNDProperties, Hashmap, HashmapProperties, AggregatorProperties};
use crate::proto;
use crate::utilities::json::{JSONRelease};
use crate::hashmap;
use itertools::Itertools;

pub trait Component {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties>;

    fn get_names(
        &self,
        properties: &NodeProperties,
    ) -> Result<Vec<String>>;
}

pub trait Expandable {
    // return a hashmap of an expanded subgraph
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)>;
}

pub trait Aggregator {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivityType
    ) -> Result<Sensitivity>;
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
        node_id: &u32,
        component: &proto::Component,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>>;
}



impl Component for proto::component::Variant {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
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
    // return a hashmap of an expanded subgraph
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
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivityType
    ) -> Result<Sensitivity> {
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
    // for json construction. Return type should be a generic serializable struct, not a String
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

pub fn sensitivity_propagation_wrapper(
    privacy_definition: &proto::PrivacyDefinition,
    data_property: ValueProperties,
    sensitivity_type: &SensitivityType,
    arraynd_sensitivity: &dyn Fn(&proto::PrivacyDefinition, &SensitivityType, &ArrayNDProperties) -> Result<Sensitivity>,
    hashmap_sensitivity: &dyn Fn(&proto::PrivacyDefinition, &SensitivityType, &HashmapProperties) -> Result<Sensitivity>,
    aggregate_sensitivity: &dyn Fn(&proto::PrivacyDefinition, &SensitivityType, &AggregatorProperties) -> Result<Sensitivity>,
) -> Result<Sensitivity> {

    match data_property {

        // input data has been partitioned, compute sensitivity for each partition
        ValueProperties::Hashmap(data_property) =>
            hashmap_sensitivity(privacy_definition, sensitivity_type, &data_property),

        // input data is a single array
        ValueProperties::ArrayND(data_property) => {
            match data_property.aggregator {

                // input data has already been aggregated
                Some(aggregator_property) =>
                    aggregate_sensitivity(privacy_definition, sensitivity_type, &aggregator_property),

                // input data has not been aggregated
                None =>
                    arraynd_sensitivity(privacy_definition, sensitivity_type, &data_property)
            }
        },
        ValueProperties::Vector2DJagged(value) =>
            return Err("sensitivity is not defined for a jagged vector".into())
    }
}
