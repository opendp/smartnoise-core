use crate::errors::*;


use crate::{proto, base, Integer};
use crate::components::{Expandable, Report};
use ndarray::arr0;

use crate::base::{IndexKey, NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, privacy_usage_to_json, AlgorithmInfo, value_to_json};
use crate::utilities::get_literal;
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;


impl Expandable for proto::DpCount {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let mut expansion = base::ComponentExpansion::default();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;

        // count
        maximum_id += 1;
        let id_count = maximum_id;
        expansion.computation_graph.insert(id_count, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                "data".into() => *component.arguments().get(&IndexKey::from("data"))
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?
            ])),
            variant: Some(proto::component::Variant::Count(proto::Count {
                distinct: self.distinct
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_count);

        if self.mechanism.to_lowercase().as_str() == "simplegeometric" {

            let count_min_id = match component.arguments().get::<IndexKey>(&"lower".into()) {
                Some(id) => *id,
                None => {
                    // count_max
                    maximum_id += 1;
                    let id_count_min = maximum_id;
                    let (patch_node, count_min_release) = get_literal(0.into(), component.submission)?;
                    expansion.computation_graph.insert(id_count_min, patch_node);
                    expansion.properties.insert(id_count_min, infer_property(&count_min_release.value, None)?);
                    expansion.releases.insert(id_count_min, count_min_release);
                    id_count_min
                }
            };

            let count_max_id = match component.arguments().get::<IndexKey>(&"upper".into()) {
                None => {
                    let num_records = match properties.get::<IndexKey>(&"data".into())
                        .ok_or("data: missing")? {
                        ValueProperties::Array(value) => value.num_records,
                        ValueProperties::Dataframe(value) => value.num_records()?,
                        _ => return Err("data: must be an array or dataframe".into())
                    };

                    let count_max = match num_records {
                        Some(num_records) => arr0(num_records as Integer).into_dyn(),
                        None => if privacy_definition.protect_elapsed_time {
                            return Err("upper must be set when protecting elapsed time".into())
                        } else {
                            arr0(Integer::MAX).into_dyn()
                        }
                    };
                    // count_max
                    maximum_id += 1;
                    let id_count_max = maximum_id;
                    let (patch_node, count_max_release) = get_literal(count_max.into(), component.submission)?;
                    expansion.computation_graph.insert(id_count_max, patch_node);
                    expansion.properties.insert(id_count_max, infer_property(&count_max_release.value, None)?);
                    expansion.releases.insert(id_count_max, count_max_release);
                    id_count_max
                }
                Some(id) => *id,
            };

            // noising
            expansion.computation_graph.insert(component_id, proto::Component {
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone()
                })),
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "data".into() => id_count,
                    "lower".into() => count_min_id,
                    "upper".into() => count_max_id
                ])),
                omit: component.omit,
                submission: component.submission,
            });
        } else {
            // noising
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
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

        Ok(expansion)
    }
}

impl Report for proto::DpCount {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        _properties: NodeProperties,
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
            submission: component.submission,
            node_id,
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
