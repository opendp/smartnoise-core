use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Expandable, Report};


use crate::base::{NodeProperties, Value, Array};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_column};


impl Expandable for proto::DpMinimum {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        _maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let dp_quantile_component = proto::Component {
            arguments: component.arguments.clone(),
            variant: Some(proto::component::Variant::DpQuantile(proto::DpQuantile {
                alpha: 0.,
                interpolation: "lower".to_string(),
                mechanism: self.mechanism.clone(),
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: false,
            batch: component.batch,
        };

        Ok(proto::ComponentExpansion {
            computation_graph: hashmap![*component_id => dp_quantile_component],
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![*component_id]
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
        variable_names: Option<&Vec<String>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let lower = data_property.lower_f64()?;
        let upper = data_property.upper_f64()?;
        let num_columns = data_property.num_columns()?;

        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());
            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMinimum".to_string(),
                variables: serde_json::json!(variable_name),
                release_info: match release.array()? {
                    Array::F64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    Array::I64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    _ => return Err("mean must be numeric".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.mechanism.clone(),
                    argument: serde_json::json!({
                        "constraint": {
                            "lowerbound": lower[column_number],
                            "upperbound": upper[column_number]
                        }
                    }),
                }
            });
        }
        Ok(Some(releases))
    }
}
