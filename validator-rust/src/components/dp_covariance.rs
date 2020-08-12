use std::convert::TryFrom;

use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};
use crate::utilities::prepend;

impl Expandable for proto::DpCovariance {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mechanism = if self.mechanism.to_lowercase().as_str() == "automatic" {
            let privacy_definition = privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition must be known"))?;

            if privacy_definition.protect_floating_point
            { "snapping" } else { "laplace" }.to_string()
        } else { self.mechanism.to_lowercase() };

        let mut expansion = base::ComponentExpansion::default();

        let arguments;
        let shape;
        let symmetric;
        match properties.get(&IndexKey::from("data")) {
            Some(data_property) => {
                let data_property = data_property.array()
                    .map_err(prepend("data:"))?.clone();

                let num_columns = data_property.num_columns()?;
                shape = vec![u32::try_from(num_columns)?, u32::try_from(num_columns)?];
                arguments = indexmap![
                    "data".into() => *component.arguments().get::<IndexKey>(&"data".into())
                        .ok_or_else(|| Error::from("data must be provided as an argument"))?
                ];
                symmetric = true;
            },
            None => {
                let left_property = properties.get::<IndexKey>(&"left".into())
                    .ok_or("left: missing")?.array()
                    .map_err(prepend("left:"))?.clone();
                let right_property = properties.get::<IndexKey>(&"right".into())
                    .ok_or("right: missing")?.array()
                    .map_err(prepend("right:"))?.clone();

                shape = vec![u32::try_from(left_property.num_columns()?)?, u32::try_from(right_property.num_columns()?)?];
                arguments = indexmap![
                    "left".into() => *component.arguments().get::<IndexKey>(&"left".into())
                        .ok_or_else(|| Error::from("left must be provided as an argument"))?,
                    "right".into() => *component.arguments().get::<IndexKey>(&"right".into())
                        .ok_or_else(|| Error::from("right must be provided as an argument"))?
                ];
                symmetric = false;
            }
        };

        // covariance
        maximum_id += 1;
        let id_covariance = maximum_id;
        expansion.computation_graph.insert(id_covariance, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(arguments)),
            variant: Some(proto::component::Variant::Covariance(proto::Covariance {
                finite_sample_correction: self.finite_sample_correction
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_covariance);

        // noise
        maximum_id += 1;
        let id_noise = maximum_id;
        expansion.computation_graph.insert(id_noise, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_covariance])),
            variant: Some(match mechanism.as_str() {
                "laplace" => proto::component::Variant::LaplaceMechanism(proto::LaplaceMechanism {
                    privacy_usage: self.privacy_usage.clone()
                }),
                "gaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    analytic: false
                }),
                "analyticgaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                    privacy_usage: self.privacy_usage.clone(),
                    analytic: true
                }),
                "snapping" => {
                    let data_property = properties.get::<base::IndexKey>(&"data".into())
                        .ok_or("data: missing")?.array()
                        .map_err(prepend("data:"))?;

                    let b: Vec<f64> = data_property.upper_float()
                        .or_else(|_| data_property.upper_int()
                            .map(|upper| upper.into_iter().map(|v| v as f64).collect()))?;

                    proto::component::Variant::SnappingMechanism(proto::SnappingMechanism {
                        privacy_usage: self.privacy_usage.clone(),
                        b,
                    })
                }
                _ => bail!("Unexpected invalid token {:?}", self.mechanism.as_str())
            }),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_noise);

        // reshape into matrix
        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_noise])),
            variant: Some(proto::component::Variant::Reshape(proto::Reshape {
                symmetric,
                layout: "row".to_string(),
                shape
            })),
            omit: component.omit,
            submission: component.submission
        });

        Ok(expansion)
    }
}

impl Report for proto::DpCovariance {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {

        let argument;
        let statistic;

        if properties.contains_key(&IndexKey::from("data")) {
            let data_property = properties.get::<IndexKey>(&"data".into())
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();

            statistic = "DPCovariance".to_string();
            argument = serde_json::json!({
                "n": data_property.num_records()?,
                "constraint": {
                    "lowerbound": data_property.lower_float()?,
                    "upperbound": data_property.upper_float()?
                }
            });
        }
        else {
            let left_property = properties.get::<IndexKey>(&"left".into())
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();
            let right_property = properties.get::<IndexKey>(&"right".into())
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();

            statistic = "DPCrossCovariance".to_string();
            argument = serde_json::json!({
                "n": left_property.num_records()?,
                "constraint": {
                    "lowerbound_left": left_property.lower_float()?,
                    "upperbound_left": left_property.upper_float()?,
                    "lowerbound_right": right_property.lower_float()?,
                    "upperbound_right": right_property.upper_float()?
                }
            });
        }

        let privacy_usage: Vec<serde_json::Value> = self.privacy_usage.iter()
            .map(privacy_usage_to_json).clone().collect();


        Ok(Some(vec![JSONRelease {
            description: "DP release information".to_string(),
            statistic,
            variables: serde_json::json!(variable_names.cloned()
                .unwrap_or_else(Vec::new).iter()
                .map(|v| v.to_string()).collect::<Vec<String>>()),
            release_info: value_to_json(&release)?,
            privacy_loss: serde_json::json![privacy_usage],
            accuracy: None,
            submission: component.submission,
            node_id,
            postprocess: false,
            algorithm_info: AlgorithmInfo {
                name: "".to_string(),
                cite: "".to_string(),
                mechanism: self.mechanism.clone(),
                argument
            }
        }]))
    }
}
