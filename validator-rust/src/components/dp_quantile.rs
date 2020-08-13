use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{Array, IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend, privacy::spread_privacy_usage};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};

impl Expandable for proto::DpQuantile {
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
        let argument_ids = component.arguments();

        let data_id = *argument_ids.get::<IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("data is a required argument to DPQuantile"))?;

        let mechanism = if self.mechanism.to_lowercase().as_str() == "automatic" {
            if properties.contains_key::<IndexKey>(&"candidates".into()) {
                "exponential"
            } else {
                let privacy_definition = privacy_definition.as_ref()
                    .ok_or_else(|| Error::from("privacy_definition must be known"))?;
                if privacy_definition.protect_floating_point { "snapping" } else { "laplace" }
            }.to_string()
        } else {
            self.mechanism.to_lowercase()
        };

        // quantile
        let mut quantile_args = indexmap![IndexKey::from("data") => data_id];
        if mechanism.as_str() == "exponential" {
            quantile_args.insert("candidates".into(), *argument_ids.get::<IndexKey>(&"candidates".into())
                .ok_or_else(|| Error::from("candidates is a required argument to DPQuantile when the exponential mechanism is used."))?);
        }
        maximum_id += 1;
        let id_quantile = maximum_id;
        expansion.computation_graph.insert(id_quantile, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(quantile_args)),
            variant: Some(proto::component::Variant::Quantile(proto::Quantile {
                alpha: self.alpha,
                interpolation: self.interpolation.clone(),
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_quantile);

        // sanitizing
        let mut sanitize_args = IndexMap::new();
        if self.mechanism.to_lowercase().as_str() == "exponential" {
            sanitize_args.insert("utilities".into(), id_quantile);
            sanitize_args.insert("candidates".into(), *argument_ids.get::<IndexKey>(&"candidates".into())
                .ok_or_else(|| Error::from("candidates is a required argument to DPQuantile when the exponential mechanism is used."))?);
        } else {
            sanitize_args.insert("data".into(), id_quantile);
        }

        let variant = Some(match mechanism.as_str() {
            "laplace" => proto::component::Variant::LaplaceMechanism(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            }),
            "analyticgaussian" => proto::component::Variant::GaussianMechanism(proto::GaussianMechanism {
                privacy_usage: self.privacy_usage.clone(),
                analytic: true
            }),
            "exponential" => proto::component::Variant::ExponentialMechanism(proto::ExponentialMechanism {
                privacy_usage: self.privacy_usage.clone()
            }),
            "exponential" => proto::component::Variant::ExponentialMechanism(proto::ExponentialMechanism {
                privacy_usage: self.privacy_usage.clone()
            }),
            "snapping" => {
                argument_ids.get::<IndexKey>(&"lower".into())
                    .map(|lower| sanitize_args.insert("lower".into(), *lower));
                argument_ids.get::<IndexKey>(&"upper".into())
                    .map(|upper| sanitize_args.insert("upper".into(), *upper));

                proto::component::Variant::SnappingMechanism(proto::SnappingMechanism {
                    privacy_usage: self.privacy_usage.clone()
                })
            },
            _ => bail!("Unexpected invalid token {:?}", self.mechanism.as_str()),
        });
        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(sanitize_args)),
            variant,
            omit: component.omit,
            submission: component.submission,
        });

        Ok(expansion)
    }
}


impl Report for proto::DpQuantile {
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

        let minimums = data_property.lower_float().unwrap();
        let maximums = data_property.upper_float().unwrap();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPQuantile".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                release_info: match release.ref_array()? {
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
