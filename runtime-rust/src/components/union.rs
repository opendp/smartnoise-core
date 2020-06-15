use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Array, IndexKey, Value};
use crate::components::Evaluable;

use whitenoise_validator::proto;
use whitenoise_validator::utilities::array::slow_stack;
use ndarray::{Axis, ArrayViewD, stack};
use indexmap::map::IndexMap;
use crate::utilities::to_nd;


impl Evaluable for proto::Union {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = arguments.into_iter()
            .map(|(key, &value)| (key.clone(), value.clone()))
            .collect::<IndexMap<IndexKey, Value>>();

        if !self.flatten {
            return Ok(ReleaseNode::new(data.into()))
        }

        let arrays = data.values()
            .map(|v| v.array()).collect::<Result<Vec<&Array>>>()?;

        Ok(ReleaseNode::new(match arrays.first().ok_or_else(|| "must have at least one partition")? {
            Array::F64(_) => {
                let inputs = arrays.iter()
                    .map(|v| v.f64().and_then(|v| to_nd(v.clone(), 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<f64>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view()).collect::<Vec<ArrayViewD<f64>>>())?.into()
            },
            Array::I64(_) => {
                let inputs = arrays.iter()
                    .map(|v| v.i64().and_then(|v| to_nd(v.clone(), 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<i64>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view()).collect::<Vec<ArrayViewD<i64>>>())?.into()
            },
            Array::Bool(_) => {
                let inputs = arrays.iter()
                    .map(|v| v.bool().and_then(|v| to_nd(v.clone(), 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<bool>>>>()?;
                stack(Axis(0), &inputs.iter().map(|v| v.view()).collect::<Vec<ArrayViewD<bool>>>())?.into()
            },
            Array::Str(_) => {
                let inputs = arrays.iter()
                    .map(|v| v.string().and_then(|v| to_nd(v.clone(), 2)))
                    .collect::<Result<Vec<ndarray::ArrayD<String>>>>()?;
                slow_stack(Axis(0), &inputs.iter().map(|v| v.view()).collect::<Vec<ArrayViewD<String>>>())?.into()
            }
        }))
    }
}
