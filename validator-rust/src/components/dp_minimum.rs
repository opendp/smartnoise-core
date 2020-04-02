use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties, Array};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_release};


impl Component for proto::DpMinimum {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPMinimum is abstract, and has no property propagation".into())
    }


}

impl Expandable for proto::DpMinimum {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        // minimum
        current_id += 1;
        let id_minimum = current_id;
        computation_graph.insert(id_minimum, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data")
                .ok_or_else(|| Error::from("data: missing"))?],
            variant: Some(proto::component::Variant::from(proto::Minimum {})),
            omit: true,
            batch: component.batch,
        });

//        let id_candidates = component.arguments.get("candidates").unwrap().clone();

        // sanitizing
        computation_graph.insert(component_id.clone(), proto::Component {
            arguments: hashmap!["data".to_owned() => id_minimum],
            variant: Some(proto::component::Variant::from(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_minimum]
        })
    }
}


impl Report for proto::DpMinimum {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value,
        variable_names: &Vec<String>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.min_f64()?;
        let maximums = data_property.max_f64()?;
        let num_columns = data_property.num_columns()?;

        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..num_columns {
            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMinimum".to_string(),
                variables: serde_json::json!(variable_names),
                release_info: match release.array()? {
                    Array::F64(v) => value_to_json(&get_ith_release(v, &(column_number as usize))?.into())?,
                    Array::I64(v) => value_to_json(&get_ith_release(v, &(column_number as usize))?.into())?,
                    _ => return Err("maximum must be numeric".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number as usize].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.implementation.clone(),
                    argument: serde_json::json!({
                        "constraint": {
                            "lowerbound": minimums[column_number as usize],
                            "upperbound": maximums[column_number as usize]
                        }
                    }),
                }
            });
        }
        Ok(Some(releases))
    }
}
