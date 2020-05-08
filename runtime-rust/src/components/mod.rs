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
pub mod kth_raw_sample_moment;
pub mod map;
pub mod materialize;
pub mod maximum;
pub mod mean;
pub mod mechanisms;
pub mod merge;
pub mod minimum;
pub mod partition;
pub mod quantile;
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
    /// * `arguments` - a hashmap, where the `String` keys are the names of arguments, and the `Value` values are the data inputs
    ///
    /// # Returns
    /// The concrete value corresponding to the abstract computation that the struct represents
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode>;
}

impl Evaluable for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn evaluate(
        &self, arguments: &NodeArguments,
    ) -> Result<ReleaseNode> {
        macro_rules! evaluate {
            ($( $variant:ident ),*) => {
                {
                    $(
                       if let proto::component::Variant::$variant(x) = self {
                            return x.evaluate(arguments)
                                .chain_err(|| format!("node specification: {:?}:", self))
                       }
                    )*
                }
            }
        }

        evaluate!(
            // INSERT COMPONENT LIST
            Cast, Clamp, Count, Covariance, Digitize, Filter, Histogram, Impute, Index,
            KthRawSampleMoment, Map, Maximum, Materialize, Mean, Merge, Minimum, Partition,
            Quantile, Rename, Reshape,

            ExponentialMechanism, LaplaceMechanism, GaussianMechanism, SimpleGeometricMechanism,

            Resize, Sum, Variance,

            Abs, Add, LogicalAnd, Divide, Equal, GreaterThan, LessThan, Log, Modulo, Multiply,
            Negate, Negative, LogicalOr, Power, RowMax, RowMin, Subtract
        );

        Err(format!("Component type not implemented: {:?}", self).into())
    }
}
