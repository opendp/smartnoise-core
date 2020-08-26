use crate::errors::*;


use crate::{proto, base, Integer};
use crate::components::{Expandable, Report};
use ndarray::{arr0};

use crate::base::{NodeProperties, Value, IndexKey};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, array::get_ith_column, get_literal, privacy::spread_privacy_usage};
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;


impl Expandable for proto::DpHistogram {
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

        let data_id = component.arguments().get::<IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("data is a required argument to DPHistogram"))?.to_owned();

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;

        // histogram
        maximum_id += 1;
        let id_histogram = maximum_id;
        let mut histogram_arguments = indexmap!["data".into() => data_id];
        let arguments = component.arguments();
        vec!["categories", "null_value", "edges", "inclusive_left"].into_iter()
            .map(|name| name.into())
            .for_each(|name| {
                arguments.get(&name)
                    .map(|v| histogram_arguments.insert(name, *v));
            });

        expansion.computation_graph.insert(id_histogram, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(histogram_arguments)),
            variant: Some(proto::component::Variant::Histogram(proto::Histogram {})),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_histogram);

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
            let count_max_id = match arguments.get::<IndexKey>(&"upper".into()) {
                Some(id) => *id,
                None => {
                    let count_max = match data_property.num_records {
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
            };

            // noising
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "data".into() => id_histogram,
                    "lower".into() => count_min_id,
                    "upper".into() => count_max_id
                ])),
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone()
                })),
                omit: component.omit,
                submission: component.submission,
            });
        } else {

            // noising
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
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

        Ok(expansion)
    }
}

impl Report for proto::DpHistogram {
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

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        let variable_names = variable_names.cloned()
            .unwrap_or_else(|| (0..num_columns).map(|_| "[Unknown]".into()).collect());

        let release = release.ref_array()?.ref_int()?;

        Ok(Some(privacy_usages.into_iter()
            .zip(variable_names.into_iter()).enumerate()
            .map(|(column_number, (privacy_usage, variable_name))|
                Ok(JSONRelease {
                    description: "DP release information".to_string(),
                    statistic: "DPHistogram".to_string(),
                    variables: serde_json::json!(variable_name.to_string()),
                    // extract ith column of release
                    release_info: value_to_json(&get_ith_column(
                        release,
                        column_number,
                    )?.into())?,
                    privacy_loss: privacy_usage_to_json(&privacy_usage),
                    accuracy: None,
                    submission: component.submission,
                    node_id,
                    postprocess: false,
                    algorithm_info: AlgorithmInfo {
                        name: "".to_string(),
                        cite: "".to_string(),
                        mechanism: self.mechanism.clone(),
                        argument: serde_json::json!({}),
                    },
                }))
            .collect::<Result<Vec<JSONRelease>>>()?))
    }
}
