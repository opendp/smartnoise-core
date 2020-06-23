use crate::errors::*;

use crate::{proto, base};
use crate::components::{Expandable, Report};

use crate::base::{NodeProperties, Value, Array};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, privacy::spread_privacy_usage, array::get_ith_column};
use indexmap::map::IndexMap;


impl Expandable for proto::DpRawMoment {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();

        let data_id = *component.arguments().get::<base::IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("data must be provided as an argument"))?;

        // kth raw moment
        maximum_id += 1;
        let id_moment = maximum_id;
        expansion.computation_graph.insert(id_moment, proto::Component {
            arguments: Some(proto::IndexmapNodeIds::new(
                indexmap!["data".into() => data_id])),
            variant: Some(proto::component::Variant::RawMoment(proto::RawMoment {
                order: self.order
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_moment);

        // noising
        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::IndexmapNodeIds::new(indexmap!["data".into() => id_moment])),
            variant: Some(match self.mechanism.to_lowercase().as_str() {
                "laplace" => proto::component::Variant::LaplaceMechanism(proto::LaplaceMechanism {
                    privacy_usage: self.privacy_usage.clone()
                }),
                "gaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                    privacy_usage: self.privacy_usage.clone()
                }),
                _ => panic!("Unexpected invalid token {:?}", self.mechanism.as_str()),
            }),
            omit: component.omit,
            submission: component.submission,
        });

        Ok(expansion)
    }
}


impl Report for proto::DpRawMoment {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: &IndexMap<base::IndexKey, &Value>,
        properties: &NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.lower_float()?;
        let maximums = data_property.upper_float()?;
        let num_records = data_property.num_records()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPRawMoment".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                release_info: match release.array()? {
                    Array::Float(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    Array::Int(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    _ => return Err("maximum must be numeric".into())
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
                            "n": num_records,
                            "constraint": {
                                "lowerbound": minimums[column_number],
                                "upperbound": maximums[column_number]
                            }
                        }),
                },
            });
        }
        Ok(Some(releases))
    }
}
