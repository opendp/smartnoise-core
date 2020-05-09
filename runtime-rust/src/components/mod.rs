//! Component trait implementations
//!
//! Each component represents an abstract computation.
//! Example components are Materialize for loading a dataframe, Index for retrieving specific columns from the dataframe, Mean for aggregating data, LaplaceMechanism for privatizing data, etc.
//!
//! There are a set of possible behaviours each component may implement. Each behavior corresponds to a trait.
//! The only trait in the runtime is the Evaluable trait.
//!
//! Implementations of the Evaluable trait are distributed among the module files.

use whitenoise_validator::errors::*;
use crate::NodeArguments;
use whitenoise_validator::base::ReleaseNode;

use whitenoise_validator::proto;

//pub mod bin;
pub mod cast;
pub mod clamp;
pub mod count;
pub mod covariance;
pub mod digitize;
pub mod filter;
pub mod histogram;
pub mod impute;
pub mod index;
pub mod map;
pub mod materialize;
pub mod mean;
pub mod mechanisms;
pub mod merge;
pub mod partition;
pub mod quantile;
pub mod raw_moment;
pub mod rename;
pub mod reshape;
pub mod resize;
pub mod sum;
pub mod transforms;
pub mod variance;

/// Evaluable component trait
///
/// Evaluable structs represent an abstract computation.
pub trait Evaluable {
    /// The concrete implementation of the abstract computation that the struct represents.
    ///
    /// # Arguments
    /// * `privacy_definition` - the definition of privacy under which the computation takes place
    /// * `arguments` - a hashmap, where the `String` keys are the names of arguments, and the `Value` values are the data inputs
    ///
    /// # Returns
    /// The concrete value corresponding to the abstract computation that the struct represents
    fn evaluate(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        arguments: &NodeArguments
    ) -> Result<ReleaseNode>;
}

impl Evaluable for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn evaluate(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        arguments: &NodeArguments
    ) -> Result<ReleaseNode> {
        macro_rules! evaluate {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.evaluate(privacy_definition, arguments)
                                .chain_err(|| format!("node specification: {:?}:", self))
                       }
                    )*
                }
            }
        }

        evaluate!(
            // INSERT COMPONENT LIST
            Cast, Clamp, Count, Covariance, Digitize, Filter, Histogram, Impute, Index,
            Map, Materialize, Mean, Merge, Partition,
            Quantile, RawMoment, Rename, Reshape, Resize, Sum, Variance,

            ExponentialMechanism, GaussianMechanism, LaplaceMechanism, SimpleGeometricMechanism,

            Abs, Add, LogicalAnd, Divide, Equal, GreaterThan, LessThan, Log, Modulo, Multiply,
            Negate, Negative, LogicalOr, Power, RowMax, RowMin, Subtract
        );

        Err(format!("Component type not implemented: {:?}", self).into())
    }
}
