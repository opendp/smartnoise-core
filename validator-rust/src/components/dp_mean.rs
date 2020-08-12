use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend, privacy::spread_privacy_usage};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};

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

        if self.implementation.to_lowercase().as_str() == "plug-in" {

            let num_columns = properties.get::<base::IndexKey>(&"data".into())
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.num_columns()? as f64;

            let id_data = *component.arguments().get::<base::IndexKey>(&"data".into())
                .ok_or_else(|| Error::from("data must be provided as an argument"))?;

            // dp sum
            maximum_id += 1;
            let id_dp_sum = maximum_id;
            expansion.computation_graph.insert(id_dp_sum, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["data".into() => id_data])),
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

            // dp count
            maximum_id += 1;
            let id_dp_count = maximum_id;
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
            maximum_id += 1;
            let id_to_float = maximum_id;
            expansion.computation_graph.insert(id_to_float, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["data".into() => id_dp_count])),
                variant: Some(proto::component::Variant::ToFloat(proto::ToFloat {})),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(id_to_float);

            // divide
            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "left".into() => id_dp_sum,
                    "right".into() => id_to_float])),
                variant: Some(proto::component::Variant::Divide(proto::Divide {})),
                omit: component.omit,
                submission: component.submission,
            });

            Ok(expansion)
        }

        else if self.implementation.to_lowercase().as_str() == "resize" {
            // mean
            maximum_id += 1;
            let id_mean = maximum_id;
            expansion.computation_graph.insert(id_mean, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                    "data".into() => *component.arguments().get::<IndexKey>(&"data".into())
                        .ok_or_else(|| Error::from("data must be provided as an argument"))?])),
                variant: Some(proto::component::Variant::Mean(proto::Mean {})),
                omit: true,
                submission: component.submission,
            });
            expansion.traversal.push(id_mean);

            // noising
            let mechanism = if self.mechanism.to_lowercase().as_str() == "automatic" {
                let privacy_definition = privacy_definition.as_ref()
                    .ok_or_else(|| Error::from("privacy_definition must be known"))?;
                if privacy_definition.protect_floating_point
                { "snapping" } else { "laplace" }.to_string()
            } else { self.mechanism.to_lowercase() };

            expansion.computation_graph.insert(component_id, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_mean])),
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
                    },
                    _ => bail!("Unexpected invalid token {:?}", self.mechanism.as_str())
                }),
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
