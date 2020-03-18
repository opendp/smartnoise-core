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

pub trait Evaluable {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value>;
}

impl Evaluable for proto::component::Variant {
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
    fn evaluate(&self, _arguments: &NodeArguments) -> Result<Value> {
        parse_value(&self.to_owned().value.unwrap())
    }
}
