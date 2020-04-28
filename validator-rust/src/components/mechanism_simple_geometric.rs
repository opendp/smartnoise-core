use crate::errors::*;

use std::collections::HashMap;
use math::round;

use crate::components::{Sensitivity, Accuracy};
use crate::{proto, base};

use crate::components::{Component, Expandable};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::{prepend, expand_mechanism, broadcast_privacy_usage, get_epsilon};


impl Component for proto::SimpleGeometricMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be integer".into())
        }

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        data_property.aggregator = None;
        data_property.releasable = true;

        Ok(data_property.into())
    }


}


impl Expandable for proto::SimpleGeometricMechanism {
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

impl Accuracy for proto::SimpleGeometricMechanism {
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

        let sensitivity_value = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_value.array()?.f64()?;

        Ok(Some(sensitivities.into_iter().zip(accuracies.values.iter())
            .map(|(sensitivity, accuracy)| proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: (1. / accuracy.alpha).ln() * (sensitivity / accuracy.value),
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

        let sensitivity_value = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_value.array()?.f64()?;

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        Ok(Some(sensitivities.into_iter().zip(epsilon.into_iter())
            .map(|(sensitivity, epsilon)| {
                let unrounded_accuracy = (1. / *alpha).ln() * (sensitivity / epsilon);
                proto::Accuracy {
                    value: round::ceil(unrounded_accuracy, 0),
                    alpha: *alpha,
            }
        }).collect()))
    }
}
