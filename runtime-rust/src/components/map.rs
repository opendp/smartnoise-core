use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Value};
use crate::components::Evaluable;

use whitenoise_validator::proto;
use std::collections::HashMap;


impl Evaluable for proto::Map {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {

        // TODO: currently unable to differentiate between dataframes and partitions
        let (args_partitioned, args_singular): (HashMap<String, &Value>, HashMap<String, &Value>) = arguments.into_iter()
            .map(|(k, v)| (k.clone(), *v))
            .partition(|(_, props)| props.indexmap().is_ok());

        let first_partition = args_partitioned.values().next()
            .ok_or_else(|| "there must be at least one partitioned argument to map")?.clone();

        let num_partitions = first_partition.indexmap()?.keys_length();

        Ok(ReleaseNode::new(Value::Indexmap(first_partition.indexmap()?
            .from_values((0..num_partitions)
                .map(|idx| {
                    let mut partition_args = args_partitioned.iter()
                        .map(|(name, map_props)| Ok((
                            name.clone(),
                            map_props.indexmap()?.values()[idx]
                        )))
                        .collect::<Result<HashMap<String, &Value>>>()?;

                    partition_args.extend(args_singular.iter()
                        .map(|(k, v)| (k.clone(), *v)));

                    self.component.as_ref().ok_or_else(|| "component must be defined")?
                        .variant.as_ref().ok_or_else(|| "variant must be defined")?
                        .evaluate(privacy_definition, &partition_args).map(|v| v.value)
                })
                .collect::<Result<Vec<Value>>>()?))))
    }
}
