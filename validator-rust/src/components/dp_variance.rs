use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{Array, IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};
use crate::utilities::privacy::spread_privacy_usage;

impl Expandable for proto::DpVariance {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        _properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();


        let argument_ids = component.arguments();

        // variance
        maximum_id += 1;
        let id_variance = maximum_id;
        expansion.computation_graph.insert(id_variance, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                "data".into() => *argument_ids.get(&IndexKey::from("data"))
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?])),
            variant: Some(proto::component::Variant::Variance(proto::Variance {
                finite_sample_correction: self.finite_sample_correction
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_variance);

        // noising
        let mechanism = if self.mechanism.to_lowercase().as_str() == "automatic" {
            let privacy_definition = privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition must be known"))?;
            if privacy_definition.protect_floating_point
            { "snapping" } else { "laplace" }.to_string()
        } else { self.mechanism.to_lowercase() };

        let mut arguments = indexmap!["data".into() => id_variance];
        let variant = Some(match mechanism.as_str() {
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
                argument_ids.get::<IndexKey>(&"lower".into())
                    .map(|lower| arguments.insert("lower".into(), *lower));
                argument_ids.get::<IndexKey>(&"upper".into())
                    .map(|upper| arguments.insert("upper".into(), *upper));

                proto::component::Variant::SnappingMechanism(proto::SnappingMechanism {
                    privacy_usage: self.privacy_usage.clone()
                })
            },
            _ => bail!("Unexpected invalid token {:?}", self.mechanism.as_str()),
        });

        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(arguments)),
            variant,
            omit: component.omit,
            submission: component.submission,
        });

        Ok(expansion)
    }
}

impl Report for proto::DpVariance {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get(&IndexKey::from("data"))
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
                statistic: "DPVariance".to_string(),
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
