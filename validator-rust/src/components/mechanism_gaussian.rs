use crate::errors::*;

use std::collections::HashMap;
use statrs::function::erf;
use ::itertools::izip;

use crate::components::{Sensitivity, Accuracy};
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::{prepend, expand_mechanism, broadcast_privacy_usage, get_epsilon, get_delta};


impl Component for proto::GaussianMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into());
        }
        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let mut sensitivity_values = aggregator.component.compute_sensitivity(
            privacy_definition.as_ref().ok_or_else(|| "privacy_definition must be defined")?,
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

        let sensitivities = sensitivity_values.array()?.f64()?;


        if self.privacy_usage.len() == 0 {
            data_property.releasable = false;
        } else {
            let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
            let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
            let deltas = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;

            // epsilons must be greater than 0 and less than 1.
            for epsilon in epsilons.into_iter() {
                if epsilon <= 0.0 || epsilon >= 1.0 {
                    return Err("epsilon: privacy parameter epsilon must be greater than 0".into());
                };
                if epsilon >= 1.0 {
                    println!("Warning: A privacy parameter of epsilon = {} is in use. Privacy is only \
                    guaranteed for the Gaussian mechanism as implemented in the rust runtime for epsilon \
                    between 0 and 1.", epsilon);
                }
            }


            // Check delta value; checks depend on whether or not number of records is statically known.
            match data_property.num_records {
                Some(n) => {
                    let n = n as f64;
                    for delta in deltas.into_iter() {
                        if delta <= 0.0 {
                            return Err("delta: privacy parameter delta must be greater than 0".into());
                        };
                        if delta > 1.0 / n {
                            println!("Warning: A large delta of delta = {} is in use.", delta);
                        }
                    }
                },
                None => {
                    for delta in deltas.into_iter() {
                        if delta <= 0.0 {
                            return Err("delta: privacy parameter delta must be greater than 0".into());
                        } else {
                            println!("Warning: Cannot determine if delta is reasonable due to statically \
                            unknown number of records.");
                        }
                    }
                }
            }

            data_property.releasable = true;
        }

        data_property.aggregator = None;

        Ok(data_property.into())
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

        Ok(Some(
            iter.map( |(sensitivity, accuracy, delta)| {
                let c: f64 = 2.0_f64 * (1.25_f64 / delta).ln();
                let sigma: f64 = c.sqrt() * sensitivity / accuracy.value;
                proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - accuracy.alpha),
                    delta: delta
                    }))
                }
            }).collect()))
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        alpha: &f64
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
            iter.map( |(sensitivity, epsilon, delta)| {
                let c: f64 = 2.0_f64 * (1.25_f64 / delta).ln();
                let sigma: f64 = c.sqrt() * sensitivity / epsilon;

                proto::Accuracy {
                    value : sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - *alpha),
                    alpha: *alpha,
                    }
                }).collect()))
    }
}
