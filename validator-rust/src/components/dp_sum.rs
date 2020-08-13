use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{Array, ArrayProperties, DataType, IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend, privacy::spread_privacy_usage};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};

impl Expandable for proto::DpSum {
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

        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        // sum
        maximum_id += 1;
        let id_sum = maximum_id;
        expansion.computation_graph.insert(id_sum, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                "data".into() => *argument_ids.get::<base::IndexKey>(&"data".into())
                    .ok_or_else(|| Error::from("data must be provided as an argument"))?])),
            variant: Some(proto::component::Variant::Sum(proto::Sum {})),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_sum);

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;
        let mechanism = get_mechanism(
            &data_property, &self.mechanism, privacy_definition.protect_floating_point)?;

        if mechanism.as_str() == "simplegeometric" {
            let sum_max_id = *argument_ids.get::<IndexKey>(&"upper".into())
                .ok_or_else(|| Error::from("upper must be defined for geometric mechanism"))?;
            let sum_min_id = *argument_ids.get::<IndexKey>(&"lower".into())
                .ok_or_else(|| Error::from("lower must be defined for geometric mechanism"))?;

            // noising
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "data".into() => id_sum,
                    "lower".into() => sum_min_id,
                    "upper".into() => sum_max_id
                ])),
                variant: Some(proto::component::Variant::SimpleGeometricMechanism(proto::SimpleGeometricMechanism {
                    privacy_usage: self.privacy_usage.clone()
                })),
                omit: component.omit,
                submission: component.submission,
            });
        } else {

            // noising
            let mut arguments = indexmap!["data".into() => id_sum];
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
                _ => bail!("Unexpected invalid token {:?}", mechanism.as_str()),
            });

            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(arguments)),
                variant,
                omit: component.omit,
                submission: component.submission,
            });
        };

        Ok(expansion)
    }
}

impl Report for proto::DpSum {
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

        let minimums = data_property.lower_float()?;
        let maximums = data_property.upper_float()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPSum".to_string(),
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

fn get_mechanism(data_property: &ArrayProperties, mechanism: &str, protect_floating_point: bool) -> Result<String> {
    let mechanism = mechanism.to_lowercase();

    Ok(if mechanism == "automatic" {
        match data_property.data_type {
            DataType::Int => "simplegeometric",
            DataType::Float => if protect_floating_point { "snapping" } else { "laplace" },
            _ => return Err("cannot sum non-integer data".into())
        }.to_string()
    } else {
        mechanism
    })

}