use smartnoise_validator::errors::*;

use smartnoise_validator::proto;
use smartnoise_validator::base::{ReleaseNode, Value};
use std::collections::{HashMap};
use smartnoise_validator::utilities::serial::{parse_release_node};


pub fn evaluate_function(
    function: &proto::Function,
    arguments: HashMap<String, Value>,
) -> Result<HashMap<String, Value>> {
    let proto::Function {
        computation_graph, release, arguments: arguments_names, outputs
    } = function;

    let release = release.clone()
        .unwrap_or_else(|| proto::Release { values: HashMap::new() }).values;
    let computation_graph = computation_graph.clone()
        .ok_or_else(|| Error::from("computation_graph must be defined"))?;

    let mut release = release.into_iter()
        .map(|(idx, release_node)| Ok((idx, parse_release_node(release_node))))
        .collect::<Result<HashMap<u32, ReleaseNode>>>()?;

    // insert arguments into function
    arguments_names.iter()
        .try_for_each(|(name, id)| {
            let argument = arguments.get(name)
                .ok_or_else(|| Error::from(format!("missing argument in function evaluation: {}", name)))?;
            release.insert(*id, ReleaseNode {
                value: argument.clone(),
                privacy_usages: None,
                public: true,
            });
            Ok::<_, Error>(())
        })?;

    let (release, warnings) = crate::release(
        None,
        computation_graph.value,
        release,
        proto::FilterLevel::All)?;

    outputs.iter()
        .map(|(name, id)| Ok((
            name.clone(),
            release.get(id)
                .ok_or_else(|| Error::from(format!("Function failed to evaluate. Warnings: {:?}", warnings)))?
                .value.clone()
        )))
        .collect::<Result<HashMap<String, Value>>>()
}
