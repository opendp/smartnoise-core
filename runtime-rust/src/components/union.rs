use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Array, IndexKey, Value};
use crate::components::Evaluable;

use whitenoise_validator::proto;
use whitenoise_validator::utilities::array::slow_stack;
use ndarray::{Axis, ArrayViewD, stack};
use indexmap::map::IndexMap;


impl Evaluable for proto::Union {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = arguments.into_iter()
            .map(|(key, &value)| (key.clone(), value.clone()))
            .collect::<IndexMap<IndexKey, Value>>();

        if self.flatten {
            return Ok(ReleaseNode::new(data.into()))
        }

        let arrays = data.values()
            .map(|v| v.array()).collect::<Result<Vec<&Array>>>()?;

        Ok(ReleaseNode::new(match arrays.first().ok_or_else(|| "must have at least one partition")? {
            Array::F64(_) => stack(Axis(0), &arrays.iter()
                .map(|v| v.f64().map(|v| v.view())).collect::<Result<Vec<ArrayViewD<f64>>>>()?)?.into(),
            Array::I64(_) => stack(Axis(0), &arrays.iter()
                .map(|v| v.i64().map(|v| v.view())).collect::<Result<Vec<ArrayViewD<i64>>>>()?)?.into(),
            Array::Bool(_) => stack(Axis(0), &arrays.iter()
                .map(|v| v.bool().map(|v| v.view())).collect::<Result<Vec<ArrayViewD<bool>>>>()?)?.into(),
            Array::Str(_) => slow_stack(Axis(0), &arrays.iter()
                .map(|v| v.string().map(|v| v.view())).collect::<Result<Vec<ArrayViewD<String>>>>()?)?.into(),
        }))
    }
}
