use yarrow_validator::errors::*;
use crate::base::NodeArguments;
use yarrow_validator::base::Value;
use yarrow_validator::utilities::serial::parse_value;

mod bin;
mod cast;
mod clamp;
mod count;
mod covariance;
mod filter;
mod impute;
mod index;
mod maximum;
mod materialize;
mod mean;
mod minimum;
mod quantile;
mod mechanisms;
mod resize;
mod row_max;
mod row_min;
mod sum;
mod transforms;
mod variance;

use yarrow_validator::proto;

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
