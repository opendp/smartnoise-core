use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Array, Value};
use crate::components::Evaluable;

use whitenoise_validator::{proto, Float, Integer};
use whitenoise_validator::utilities::array::slow_stack;
use ndarray::{Axis, ArrayViewD, stack};
use crate::utilities::to_nd;


impl Evaluable for proto::Union {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: NodeArguments) -> Result<ReleaseNode> {

        if !self.flatten {
            return Ok(ReleaseNode::new(Value::Partitions(arguments)))
        }

        let arrays = arguments.into_iter()
            .map(|(_, v)| v.array()).collect::<Result<Vec<Array>>>()?;

        Ok(ReleaseNode::new(match arrays.first().ok_or_else(|| "must have at least one partition")? {
            Array::Float(_) => {
                let inputs = arrays.into_iter()
                    .map(|v| v.float().and_then(|v| to_nd(v, 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<Float>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view())
                    .collect::<Vec<ArrayViewD<Float>>>())?.into()
            },
            Array::Int(_) => {
                let inputs = arrays.into_iter()
                    .map(|v| v.int().and_then(|v| to_nd(v, 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<Integer>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view())
                    .collect::<Vec<ArrayViewD<Integer>>>())?.into()
            },
            Array::Bool(_) => {
                let inputs = arrays.into_iter()
                    .map(|v| v.bool().and_then(|v| to_nd(v, 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<bool>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view())
                    .collect::<Vec<ArrayViewD<bool>>>())?.into()
            },
            Array::Str(_) => {
                let inputs = arrays.into_iter()
                    .map(|v| v.string().and_then(|v| to_nd(v, 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<String>>>>()?;
                slow_stack(Axis(0), &inputs.iter().map(|v| v.view())
                    .collect::<Vec<ArrayViewD<String>>>())?.into()
            }
        }))
    }
}
