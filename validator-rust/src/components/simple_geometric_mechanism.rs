use crate::errors::*;

use crate::components::{Sensitivity, Accuracy, Mechanism};
use crate::{proto, base, Warnable};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType, NodeProperties, IndexKey};
use crate::utilities::{prepend, expand_mechanism, get_literal};
use crate::utilities::privacy::{spread_privacy_usage, get_epsilon, privacy_usage_check};
use itertools::Itertools;
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;
use ndarray::arr1;


impl Component for proto::SimpleGeometricMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into())
        }

        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::Int {
            return Err("data: atomic type must be integer".into())
        }

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        let privacy_usage = self.privacy_usage.iter().cloned().map(Ok)
            .fold1(|l, r| l? + r?).ok_or_else(|| "privacy_usage: must be defined")??;

        let warnings = privacy_usage_check(
            &privacy_usage,
            data_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        data_property.releasable = true;
        data_property.aggregator = None;

        Ok(Warnable(data_property.into(), warnings))
    }
}


impl Expandable for proto::SimpleGeometricMechanism {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let lower_id = if public_arguments.contains_key::<IndexKey>(&"lower".into()) {
            None
        } else {
            maximum_id += 1;
            Some(maximum_id)
        };

        let upper_id = if public_arguments.contains_key::<IndexKey>(&"upper".into()) {
            None
        } else {
            maximum_id += 1;
            Some(maximum_id)
        };

        let mut expansion = expand_mechanism(
            &SensitivitySpace::KNorm(1),
            privacy_definition,
            self.privacy_usage.as_ref(),
            component,
            properties,
            component_id,
            maximum_id
        )?;

        if lower_id.is_some() || upper_id.is_some() {
            let mut component = expansion.computation_graph.get(&component_id).unwrap().clone();

            let data_property = properties.get::<IndexKey>(&"data".into())
                .ok_or("data: missing")?.array()?.clone();

            if let Some(lower_id) = lower_id {
                let (patch_node, release) = get_literal(arr1(&data_property.lower_int()?).into_dyn().into(), component.submission)?;
                expansion.computation_graph.insert(lower_id, patch_node);
                expansion.properties.insert(lower_id, infer_property(&release.value, None)?);
                expansion.releases.insert(lower_id, release);
                component.insert_argument(&"lower".into(), lower_id);
            }

            if let Some(upper_id) = upper_id {
                let (patch_node, release) = get_literal(arr1(&data_property.upper_int()?).into_dyn().into(), component.submission)?;
                expansion.computation_graph.insert(upper_id, patch_node);
                expansion.properties.insert(upper_id, infer_property(&release.value, None)?);
                expansion.releases.insert(upper_id, release);
                component.insert_argument(&"upper".into(), upper_id);
            }
            expansion.computation_graph.insert(component_id, component);
        }
        Ok(expansion)
    }
}

impl Mechanism for proto::SimpleGeometricMechanism {
    fn get_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        release_usage: Option<&Vec<proto::PrivacyUsage>>,
        properties: &NodeProperties
    ) -> Result<Option<Vec<proto::PrivacyUsage>>> {

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        Some(release_usage.unwrap_or_else(|| &self.privacy_usage).iter()
            .zip(data_property.c_stability.iter())
            .map(|(usage, c_stab)|
                usage.effective_to_actual(1., *c_stab as f64, privacy_definition.group_size))
            .collect::<Result<Vec<proto::PrivacyUsage>>>()).transpose()
    }
}


impl Accuracy for proto::SimpleGeometricMechanism {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        accuracies: &proto::Accuracies,
    ) -> Result<Option<Vec<proto::PrivacyUsage>>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.float()?;

        Ok(Some(sensitivities.into_iter().zip(accuracies.values.iter())
            .map(|(sensitivity, accuracy)| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: (1. / accuracy.alpha).ln() * (*sensitivity as f64 / accuracy.value),
                    delta: 0.,
                }))
            })
            .collect()))
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        alpha: f64
    ) -> Result<Option<Vec<proto::Accuracy>>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        Ok(Some(sensitivities.into_iter().zip(epsilon.into_iter())
            .map(|(sensitivity, epsilon)| proto::Accuracy {
                value: ((1. / alpha).ln() * (*sensitivity as f64 / epsilon)).ceil(),
                alpha
            }).collect()))
    }
}
