use whitenoise_validator::errors::*;
use crate::base::NodeArguments;
use whitenoise_validator::base::Value;
use whitenoise_validator::utilities::serial::parse_value;

pub mod bin;
pub mod cast;
pub mod clamp;
pub mod count;
pub mod covariance;
pub mod filter;
pub mod impute;
pub mod index;
pub mod maximum;
pub mod materialize;
pub mod mean;
pub mod minimum;
pub mod quantile;
pub mod mechanisms;
pub mod resize;
pub mod row_max;
pub mod row_min;
pub mod sum;
pub mod transforms;
pub mod variance;

use whitenoise_validator::proto;

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
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value>;
}

impl Evaluable for proto::component::Variant {
    /// Utility implementation on the enum containing all variants of a component.
    ///
    /// This utility delegates evaluation to the concrete implementation of each component variant.
    fn evaluate(
        &self, arguments: &NodeArguments
    ) -> Result<Value> {

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
            Constant, Bin, Cast, Clamp, Count, Covariance, Filter, Impute, Index, Maximum, Materialize, Mean,
            Minimum, Quantile, Laplacemechanism, Gaussianmechanism, Simplegeometricmechanism, Resize,
            Sum, Variance,

            Add, Subtract, Divide, Multiply, Power, Log, Modulo, Remainder, And, Or, Negate,
            Equal, Lessthan, Greaterthan, Negative
        );

        Err(format!("Component type not implemented: {:?}", self).into())

    }
}


impl Evaluable for proto::Constant {
    /// Deprecated. "Evaluate" by returning a precomputed Value stored in the description of computation (self).
    fn evaluate(&self, _arguments: &NodeArguments) -> Result<Value> {
        parse_value(&self.to_owned().value.unwrap())
    }
}
