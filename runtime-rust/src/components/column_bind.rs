use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Array, ReleaseNode};
use crate::components::Evaluable;

use whitenoise_validator::{proto};
use crate::utilities::{to_nd};
use ndarray::stack;
use whitenoise_validator::utilities::array::slow_stack;

impl Evaluable for proto::ColumnBind {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: NodeArguments) -> Result<ReleaseNode> {

        let arrays = arguments.into_iter()
            .map(|(_, v)| v.array()).collect::<Result<Vec<Array>>>()?;

        macro_rules! col_stack {
            ($func:ident, $method:ident) => {
                {
                    let inputs = arrays.into_iter()
                        .map(|v| v.$method().and_then(|v| to_nd(v, 2)))
                        .collect::<Result<Vec<ndarray::ArrayD<_>>>>()?;
                    $func(Axis(1), &inputs.iter().map(|v| v.view())
                        .collect::<Vec<ArrayViewD<_>>>())?.into()
                }
            }
        }

        Ok(ReleaseNode::new(match arrays.first().ok_or_else(|| "must have at least one argument")? {
            Array::Float(_) => col_stack!(stack, float),
            Array::Int(_) => col_stack!(stack, int),
            Array::Bool(_) => col_stack!(stack, bool),
            Array::Str(_) => col_stack!(slow_stack, string)
        }))
    }
}