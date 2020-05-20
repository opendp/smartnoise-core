use crate::errors::*;

use std::collections::HashMap;
use statrs::function::erf;
use ::itertools::izip;

use crate::components::{Sensitivity, Accuracy};
use crate::{proto, base, Warnable};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::{prepend, expand_mechanism};
use crate::utilities::privacy::{broadcast_privacy_usage, get_epsilon, get_delta, privacy_usage_reducer, privacy_usage_check};
use itertools::Itertools;


impl Component for proto::GaussianMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into());
        }

        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::F64 {
            return Err("data: atomic type must be float".into());
        }
        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let mut sensitivity_values = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(2))?;

        if aggregator.lipschitz_constant.iter().any(|v| v != &1.) {
            let mut sensitivity = sensitivity_values.array()?.f64()?.clone();
            sensitivity.gencolumns_mut().into_iter()
                .zip(aggregator.lipschitz_constant.iter())
                .for_each(|(mut sens, cons)| sens.iter_mut()
                    .for_each(|v| *v *= cons.powi(2)));
            sensitivity_values = sensitivity.into();
        }

        // check that sensitivity is an f64 array
        sensitivity_values.array()?.f64()?;

        let privacy_usage = self.privacy_usage.iter().cloned()
            .fold1(|l, r| privacy_usage_reducer(&l, &r, |l, r| l + r)).unwrap();

        let mut warnings = privacy_usage_check(
            &privacy_usage,
            data_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        let epsilon = get_epsilon(&privacy_usage)?;
        if epsilon > 1.0 {
            let message = Error::from(format!("Warning: A privacy parameter of epsilon = {} is in use. Privacy is only \
                    guaranteed for the Gaussian mechanism as implemented in the rust runtime for epsilon \
                    between 0 and 1.", epsilon));

            if privacy_definition.strict_parameter_checks {
                return Err(message)
            }
            warnings.push(message);
        }

        if get_delta(&privacy_usage)? == 0.0 {
            return Err("delta: may not be zero".into())
        }

        data_property.releasable = true;
        data_property.aggregator = None;

        Ok(Warnable(data_property.into(), warnings))
    }
}

impl Expandable for proto::GaussianMechanism {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        expand_mechanism(
            &SensitivitySpace::KNorm(2),
            privacy_definition,
            component,
            properties,
            component_id,
            maximum_id,
        )
    }
}

impl Accuracy for proto::GaussianMechanism {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        accuracies: &proto::Accuracies,
    ) -> Result<Option<Vec<proto::PrivacyUsage>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(2))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.f64()?;
        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;
        let iter = izip!(sensitivities.into_iter(), accuracies.values.iter(), delta.into_iter());

        use proto::privacy_usage::{Distance, DistanceApproximate};

        Ok(Some(
            iter.map(|(sensitivity, accuracy, delta)| {
                let c: f64 = 2.0_f64 * (1.25_f64 / delta).ln();
                let sigma: f64 = c.sqrt() * sensitivity / accuracy.value;
                proto::PrivacyUsage {
                    distance: Some(Distance::Approximate(DistanceApproximate {
                        epsilon: sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - accuracy.alpha),
                        delta,
                    }))
                }
            }).collect()))
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        alpha: &f64,
    ) -> Result<Option<Vec<proto::Accuracy>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.f64()?;

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let deltas = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;
        let iter = izip!(sensitivities.into_iter(), epsilons.into_iter(), deltas.into_iter());

        Ok(Some(
            iter.map(|(sensitivity, epsilon, delta)| {
                let c: f64 = 2.0_f64 * (1.25_f64 / delta).ln();
                let sigma: f64 = c.sqrt() * sensitivity / epsilon;

                proto::Accuracy {
                    value: sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - *alpha),
                    alpha: *alpha,
                }
            }).collect()))
    }
}
