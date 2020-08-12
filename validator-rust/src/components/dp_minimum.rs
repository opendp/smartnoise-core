use crate::errors::*;


use crate::{proto, base};
use crate::components::{Expandable, Report};


use crate::base::{NodeProperties, Value, Array, IndexKey};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};
use crate::utilities::{prepend, privacy::spread_privacy_usage, array::get_ith_column};
use indexmap::map::IndexMap;


impl Expandable for proto::DpMinimum {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        _properties: &base::NodeProperties,
        component_id: u32,
        mut _maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let mut expansion = base::ComponentExpansion::default();

        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: component.arguments.clone(),
            variant: Some(proto::component::Variant::DpQuantile(proto::DpQuantile {
                alpha: 0.,
                interpolation: "lower".to_string(),
                mechanism: self.mechanism.clone(),
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: component.omit,
            submission: component.submission,
        });
        expansion.traversal.push(component_id);

        Ok(expansion)
    }
}


impl Report for proto::DpMinimum {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let lower = data_property.lower_float()?;
        let upper = data_property.upper_float()?;
        let num_columns = data_property.num_columns()?;

        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());
            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMinimum".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                release_info: match release.ref_array()? {
                    Array::Float(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    Array::Int(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    _ => return Err("mean must be numeric".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                submission: component.submission,
                node_id,
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
