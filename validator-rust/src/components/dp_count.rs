use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::components::{Expandable, Report};
use ndarray::arr0;

use crate::base::{IndexKey, NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, privacy_usage_to_json, AlgorithmInfo, value_to_json};
use crate::utilities::get_literal;
use indexmap::map::IndexMap;


impl Expandable for proto::DpCount {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        // count
        maximum_id += 1;
        let id_count = maximum_id;
        computation_graph.insert(id_count.clone(), proto::Component {
            arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                "data".into() => *component.arguments().get(&IndexKey::from("data"))
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?
            ])),
            variant: Some(proto::component::Variant::Count(proto::Count {
                distinct: self.distinct
            })),
            omit: true,
            submission: component.submission,
        });

        if self.mechanism.to_lowercase().as_str() == "simplegeometric" {

            let count_min_id = match component.arguments().get::<IndexKey>(&"lower".into()) {
                Some(id) => id.clone(),
                None => {
                    // count_max
                    maximum_id += 1;
                    let id_count_min = maximum_id;
                    let (patch_node, count_min_release) = get_literal(0.into(), &component.submission)?;
                    computation_graph.insert(id_count_min.clone(), patch_node);
                    releases.insert(id_count_min.clone(), count_min_release);
                    id_count_min
                }
            };

            let count_max_id = match component.arguments().get::<IndexKey>(&"upper".into()) {
                Some(id) => id.clone(),
                None => {
                    let num_records = match properties.get::<IndexKey>(&"data".into())
                        .ok_or("data: missing")? {
                        ValueProperties::Array(value) => value.num_records,
                        ValueProperties::Indexmap(value) => value.num_records()?,
                        _ => return Err("data: must not be hashmap".into())
                    };

                    let count_max = match num_records {
                        Some(num_records) => arr0(num_records).into_dyn(),
                        None => match self.enforce_constant_time {
                            true => return Err("upper must be set when enforcing constant time".into()),
                            false => arr0(std::i64::MAX).into_dyn()
                        }
                    };
                    // count_max
                    maximum_id += 1;
                    let id_count_max = maximum_id;
                    let (patch_node, count_max_release) = get_literal(count_max.into(), &component.submission)?;
                    computation_graph.insert(id_count_max.clone(), patch_node);
                    releases.insert(id_count_max.clone(), count_max_release);
                    id_count_max
                }
            };

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                    "data".into() => id_count,
                    "lower".into() => count_min_id,
                    "upper".into() => count_max_id
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
                arguments: Some(proto::IndexmapNodeIds::new(
                    indexmap!["data".into() => id_count])),
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
        }

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: vec![id_count],
            warnings: vec![]
        })
    }
}

impl Report for proto::DpCount {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &IndexMap<base::IndexKey, &Value>,
        _properties: &NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        Ok(Some(vec![JSONRelease {
            description: "DP release information".to_string(),
            statistic: "DPCount".to_string(),
            variables: serde_json::json!(variable_names.cloned()
                .unwrap_or_else(Vec::new).iter()
                .map(|v| v.to_string()).collect::<Vec<String>>()),
            release_info: value_to_json(&release)?,
            privacy_loss: privacy_usage_to_json(&self.privacy_usage[0].clone()),
            accuracy: None,
            submission: component.submission as u64,
            node_id: *node_id as u64,
            postprocess: false,
            algorithm_info: AlgorithmInfo {
                name: "".to_string(),
                cite: "".to_string(),
                mechanism: self.mechanism.clone(),
                argument: serde_json::json!({
                    "distinct": self.distinct
                }),
            },
        }]))
    }
}
