use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};
use crate::components::{Expandable, Report};

use crate::base::{NodeProperties, Value, Array, IndexKey};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, privacy::spread_privacy_usage, get_ith_column};
use indexmap::map::IndexMap;

impl Expandable for proto::DpSum {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        // sum
        maximum_id += 1;
        let id_sum = maximum_id;
        computation_graph.insert(id_sum, proto::Component {
            arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                "data".into() => *component.arguments().get::<base::IndexKey>(&"data".into())
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?])),
            variant: Some(proto::component::Variant::Sum(proto::Sum {})),
            omit: true,
            submission: component.submission,
        });

        if self.mechanism.to_lowercase().as_str() == "simplegeometric" {
            let arguments = component.arguments();
            let sum_max_id = *arguments.get::<IndexKey>(&"upper".into())
                .ok_or_else(|| Error::from("upper must be defined for geometric mechanism"))?;
            let sum_min_id = *arguments.get::<IndexKey>(&"lower".into())
                .ok_or_else(|| Error::from("lower must be defined for geometric mechanism"))?;

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                    "data".into() => id_sum,
                    "lower".into() => sum_min_id,
                    "upper".into() => sum_max_id
                ])),
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    enforce_constant_time: false,
                })),
                omit: component.omit,
                submission: component.submission,
            });
        } else {

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                    "data".into() => id_sum
                ])),
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
        };

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_sum],
            warnings: vec![]
        })
    }
}

impl Report for proto::DpSum {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &IndexMap<base::IndexKey, Value>,
        properties: &NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<String>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.lower_f64()?;
        let maximums = data_property.upper_f64()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".to_string());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPSum".to_string(),
                variables: serde_json::json!(variable_name),
                release_info: match release.array()? {
                    Array::F64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    Array::I64(v) => value_to_json(&get_ith_column(v, &column_number)?.into())?,
                    _ => return Err("maximum must be numeric".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                submission: component.submission as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.mechanism.clone(),
                    argument: serde_json::json!({
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
