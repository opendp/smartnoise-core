use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend, privacy::spread_privacy_usage, get_literal};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};
use crate::utilities::inference::infer_property;

impl Expandable for proto::DpMean {
    /// Expand component
    /// # Arguments
    /// * `&self` - this
    /// * `_privacy_definition` - privacy definition from protocol buffer descriptor
    /// * `component` - component from prototypes/components.proto
    /// * `_properties` - NodeProperties
    /// * `component_id` - identifier for component from prototypes/components.proto
    /// * `maximum_id` - last ID value created for sequence, increment used to define current ID
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

        let mechanism = if self.mechanism.to_lowercase().as_str() == "automatic" {
            let privacy_definition = privacy_definition.as_ref()
                .ok_or_else(|| Error::from("privacy_definition must be known"))?;
            if privacy_definition.protect_floating_point
            { "snapping" } else { "laplace" }.to_string()
        } else { self.mechanism.to_lowercase() };

        if self.implementation.to_lowercase() == "plug-in" {

            let data_property = properties.get::<base::IndexKey>(&"data".into())
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?;
            let num_columns = data_property.num_columns()? as f64;

            let id_data = *argument_ids.get::<base::IndexKey>(&"data".into())
                .ok_or_else(|| Error::from("data must be provided as an argument"))?;

            // dp count
            maximum_id += 1;
            let mut id_dp_count = maximum_id;
            expansion.computation_graph.insert(id_dp_count, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["data".into() => id_data])),
                variant: Some(proto::component::Variant::DpCount(proto::DpCount {
                    distinct: false,
                    mechanism: "SimpleGeometric".to_string(),
                    privacy_usage: self.privacy_usage.iter().cloned()
                        .map(|v| v * (num_columns / (num_columns + 1.)))
                        .collect::<Result<Vec<proto::PrivacyUsage>>>()?
                })),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(id_dp_count);

            // to float
            if mechanism != "simplegeometric" {
                maximum_id += 1;
                expansion.computation_graph.insert(maximum_id, proto::Component {
                    arguments: Some(proto::ArgumentNodeIds::new(
                        indexmap!["data".into() => id_dp_count])),
                    variant: Some(proto::component::Variant::ToFloat(proto::ToFloat {})),
                    omit: true,
                    submission: component.submission,
                });
                expansion.traversal.push(maximum_id);
                id_dp_count = maximum_id;
            }

            // one
            maximum_id += 1;
            let id_one = maximum_id;
            let (patch_node, zero_release) = get_literal(if mechanism == "simplegeometric" {1.into()} else {1.0.into()}, component.submission)?;
            expansion.computation_graph.insert(id_one, patch_node);
            expansion.properties.insert(id_one, infer_property(&zero_release.value, None, id_one)?);
            expansion.releases.insert(id_one, zero_release);

            // set lower bound on dp count
            maximum_id += 1;
            expansion.computation_graph.insert(maximum_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["left".into() => id_dp_count, "right".into() => id_one])),
                variant: Some(proto::component::Variant::RowMax(proto::RowMax {})),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(maximum_id);
            id_dp_count = maximum_id;

            let mut dp_sum_arguments = indexmap!["data".into() => id_data];

            // if snapping or geometric, derive lower bound for statistic
            if mechanism == "snapping" || mechanism == "simplegeometric" {

                // data lower
                maximum_id += 1;
                let id_data_lower = maximum_id;
                let (patch_node, data_lower_release) = get_literal(Value::Array(data_property.lower()?), component.submission)?;
                expansion.computation_graph.insert(id_data_lower, patch_node);
                expansion.properties.insert(id_data_lower, infer_property(&data_lower_release.value, None, id_data_lower)?);
                expansion.releases.insert(id_data_lower, data_lower_release);

                // statistic lower bound
                maximum_id += 1;
                let id_sum_lower = maximum_id;
                expansion.computation_graph.insert(id_sum_lower, proto::Component {
                    arguments: Some(proto::ArgumentNodeIds::new(
                        indexmap!["left".into() => id_dp_count, "right".into() => id_data_lower])),
                    variant: Some(proto::component::Variant::Multiply(proto::Multiply {})),
                    omit: true,
                    submission: component.submission,
                });
                expansion.traversal.push(id_sum_lower);
                dp_sum_arguments.insert("lower".into(), id_sum_lower);

                // data upper
                maximum_id += 1;
                let id_data_upper = maximum_id;
                let (patch_node, data_upper_release) = get_literal(Value::Array(data_property.upper()?), component.submission)?;
                expansion.computation_graph.insert(id_data_upper, patch_node);
                expansion.properties.insert(id_data_upper, infer_property(&data_upper_release.value, None, id_data_upper)?);
                expansion.releases.insert(id_data_upper, data_upper_release);

                // statistic upper bound
                maximum_id += 1;
                let id_sum_upper = maximum_id;
                expansion.computation_graph.insert(id_sum_upper, proto::Component {
                    arguments: Some(proto::ArgumentNodeIds::new(
                        indexmap!["left".into() => id_dp_count, "right".into() => id_data_upper])),
                    variant: Some(proto::component::Variant::Multiply(proto::Multiply {})),
                    omit: true,
                    submission: component.submission,
                });
                expansion.traversal.push(id_sum_upper);
                dp_sum_arguments.insert("upper".into(), id_sum_upper);
            };

            // dp sum
            maximum_id += 1;
            let id_dp_sum = maximum_id;
            expansion.computation_graph.insert(id_dp_sum, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(dp_sum_arguments)),
                variant: Some(proto::component::Variant::DpSum(proto::DpSum {
                    mechanism: self.mechanism.clone(),
                    privacy_usage: self.privacy_usage.iter().cloned()
                        .map(|v| v / (num_columns + 1.))
                        .collect::<Result<Vec<proto::PrivacyUsage>>>()?
                })),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(id_dp_sum);

            // divide
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "left".into() => id_dp_sum,
                    "right".into() => id_dp_count
                ])),
                variant: Some(proto::component::Variant::Divide(proto::Divide {})),
                omit: component.omit,
                submission: component.submission,
            });

            Ok(expansion)
        }

        else if self.implementation.to_lowercase() == "resize" {
            // mean
            maximum_id += 1;
            let id_mean = maximum_id;
            expansion.computation_graph.insert(id_mean, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "data".into() => *argument_ids.get::<IndexKey>(&"data".into())
                        .ok_or_else(|| Error::from("data must be provided as an argument"))?])),
                variant: Some(proto::component::Variant::Mean(proto::Mean {})),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(id_mean);

            // noising
            let mut arguments = indexmap!["data".into() => id_mean];
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
                _ => bail!("Unexpected invalid token {:?}", self.mechanism.as_str())
            });

            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(arguments)),
                variant,
                omit: component.omit,
                submission: component.submission,
            });

            Ok(expansion)
        }

        else {
            bail!("`{}` is not recognized as a valid implementation. Must be one of [`resize`, `plug-in`]", self.implementation)
        }
    }
}

impl Report for proto::DpMean {
    /// summarize results
    /// # Arguments
    /// * `&self` - this
    /// * `node_id` - identifier for node
    /// * `component` - component from prototypes/components.proto
    /// * `public_arguments` - HashMap of String, Value public arguments
    /// * `properties` - NodeProperties
    /// * `release` - JSONRelease containing DP release information
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

        let lower = data_property.lower_float()?;
        let upper = data_property.upper_float()?;
        let num_records = data_property.num_records()?;

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        let release = release.ref_array()?.ref_float()?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMean".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                release_info: value_to_json(&get_ith_column(
                    release,
                    column_number as usize
                )?.into())?,
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
                        // TODO: AlgorithmInfo -> serde_json::Value, move implementation into algorithm_info
                        "implementation": self.implementation.clone(),
                        "n": num_records,
                        "constraint": {
                            "lowerbound": lower[column_number],
                            "upperbound": upper[column_number]
                        }
                    })
                }
            });
        }
        Ok(Some(releases))
    }
}
