use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::components::{Expandable, Report};
use ndarray::{arr0};

use crate::base::{NodeProperties, Value, IndexKey};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, array::get_ith_column, get_literal, privacy::spread_privacy_usage};
use indexmap::map::IndexMap;


impl Expandable for proto::DpHistogram {
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

        let data_id = component.arguments().get::<IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("data is a required argument to DPHistogram"))?.to_owned();

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        // histogram
        maximum_id += 1;
        let id_histogram = maximum_id;
        let mut histogram_arguments = indexmap!["data".into() => data_id];
        let arguments = component.arguments();
        vec!["categories", "null_value", "edges", "inclusive_left"].into_iter()
            .map(|name| name.into())
            .for_each(|name| {arguments.get(&name)
                    .map(|v| histogram_arguments.insert(name, *v));});

        computation_graph.insert(id_histogram, proto::Component {
            arguments: Some(proto::IndexmapNodeIds::new(histogram_arguments)),
            variant: Some(proto::component::Variant::Histogram(proto::Histogram {})),
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
            let count_max_id = match arguments.get::<IndexKey>(&"upper".into()) {
                Some(id) => id.clone(),
                None => {
                    let count_max = match data_property.num_records {
                        Some(num_records) => arr0(num_records).into_dyn(),
                        None => match self.enforce_constant_time {
                            true => return Err("upper must be set when enforcing constant time".into()),
                            false => arr0(std::i64::MAX).into_dyn()
                        }
                    };
                    // count_max
                    maximum_id += 1;
                    let max_id = maximum_id;
                    let (patch_node, count_max_release) = get_literal(count_max.into(), &component.submission)?;
                    computation_graph.insert(max_id.clone(), patch_node);
                    releases.insert(max_id.clone(), count_max_release);
                    max_id
                }
            };

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                    "data".into() => id_histogram,
                    "lower".into() => count_min_id,
                    "upper".into() => count_max_id
                ])),
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    enforce_constant_time: false
                })),
                omit: component.omit,
                submission: component.submission,
            });
        } else {

            // noising
            computation_graph.insert(*component_id, proto::Component {
                arguments: Some(proto::IndexmapNodeIds::new(indexmap![
                    "data".into() => id_histogram
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
        }


        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: vec![id_histogram],
            warnings: vec![]
        })
    }
}

impl Report for proto::DpHistogram {
    fn summarize(
        &self,
        node_id: &u32,
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

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPHistogram".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                // extract ith column of release
                release_info: value_to_json(&get_ith_column(
                    release.array()?.i64()?,
                    &column_number
                )?.into())?,
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                submission: component.submission as u64,
                node_id: *node_id as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.mechanism.clone(),
                    argument: serde_json::json!({}),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
