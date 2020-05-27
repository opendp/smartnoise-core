use crate::errors::*;


use std::collections::HashMap;


use crate::components::{Sensitivity, Accuracy};
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::{prepend, expand_mechanism, broadcast_privacy_usage, get_epsilon};

/// Finds precision necessary to run Snapping mechanism.
/// 
/// # Arguments
/// * `B` - Upper bound on function value being privatized.
/// 
/// # Returns
/// Gets necessary precision for Snapping mechanism.
pub fn get_precision(B: &f64) -> u32 {
    let precision: u32;
    if (B <= &(2_u32.pow(66) as f64)) {
        precision = 118;
    } else {
        let (t, k) = get_smallest_greater_or_eq_power_of_two(&B);
        precision = 118 + (k as u32) - 66;
    }
    return precision;
}

impl Component for proto::SnappingMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into())
        }

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        let sensitivities = sensitivity_values.array()?.f64()?;

        if self.privacy_usage.len() == 0 {
            data_property.releasable = false;
        } else {
            let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
            let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

            // epsilons must be greater than 0.
            for epsilon in epsilons.into_iter(){
                if epsilon <= 0.0 {
                    return Err("epsilon: privacy parameter epsilon must be greater than 0".into());
                };
                if epsilon > 1.0   {
                    println!("Warning: A large privacy parameter of epsilon = {} is in use", epsilon.to_string());
                }
            }

            data_property.releasable = true;
        }

        data_property.aggregator = None;

        Ok(data_property.into())
    }
}


impl Expandable for proto::SnappingMechanism {
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        expand_mechanism(
            &SensitivitySpace::KNorm(1),
            privacy_definition,
            component,
            properties,
            component_id,
            maximum_id
        )
    }
}


impl Accuracy for proto::SnappingMechanism {
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
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.f64()?;

        // find necessary precision
        let precision = get_precision(&self.B);

        Ok(Some(sensitivities.into_iter().zip(accuracies.values.iter())
            .map(|(sensitivity, accuracy)| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: ( (1.0 + 12.0 * self.B * 2_f64.powf(-(*precision as f64))) / accuracy.value) * (1.0 + (1.0 / accuracy.alpha).ln())
                                * (sensitivity) + 2_f64.powf(-(*precision as f64) + 1.),
                    delta: 0.,
                }))
            })
            .collect()))
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

        // find necessary precision
        let precision = get_precision(&self.B);

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        Ok(Some(sensitivities.into_iter().zip(epsilons.into_iter())
            .map(|(sensitivity, epsilon)| proto::Accuracy {
                value: ( (1.0 + 12.0 * self.B * 2_f64.powf(-(*precision as f64))) / (epsilon - 2_f64.powf(-(*precision as f64) + 1.)) )
                         * (1.0 + (1.0 / alpha).ln()) * (sensitivity),
                alpha: *alpha,
            })
            .collect()))
    }
}