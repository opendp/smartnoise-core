use indexmap::map::IndexMap;
use itertools::Itertools;
use ndarray;

use crate::{base, proto, Warnable};
use crate::base::{DataType, IndexKey, NodeProperties, SensitivitySpace, Value, ValueProperties};
use crate::components::{Mechanism, Sensitivity, Accuracy};
use crate::components::{Component, Expandable};
use crate::errors::*;
use crate::utilities::{expand_mechanism, get_literal, prepend, standardize_numeric_argument};
use crate::utilities::inference::infer_property;
use crate::utilities::privacy::{privacy_usage_check, spread_privacy_usage, get_epsilon};

impl Component for proto::SnappingMechanism {
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

        properties.get(&IndexKey::from("lower"))
            .ok_or_else(|| Error::from("lower: missing"))?;
        properties.get(&IndexKey::from("upper"))
            .ok_or_else(|| Error::from("upper: missing"))?;

        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::Float && data_property.data_type != DataType::Int {
            return Err("data: atomic type must be numeric".into());
        }

        if privacy_definition.protect_floating_point && data_property.data_type == DataType::Int {
            return Err("data: snapping may not operate on integers when floating-point protections are enabled. Use the geometric mechanism instead.".into())
        }

        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?.array()?.float()?;

        // make sure lipschitz constants is available as a float array
        aggregator.lipschitz_constants.array()?.float()?;

        let privacy_usage = self.privacy_usage.iter().cloned().map(Ok)
            .fold1(|l, r| l? + r?)
            .ok_or_else(|| "privacy_usage: must be defined")??;

        let warnings = privacy_usage_check(
            &privacy_usage,
            data_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        data_property.releasable = true;
        data_property.aggregator = None;

        Ok(Warnable(data_property.into(), warnings))
    }
}


impl Expandable for proto::SnappingMechanism {
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
                let (patch_node, release) = get_literal(Value::Array(data_property.lower()?), component.submission)?;
                expansion.computation_graph.insert(lower_id, patch_node);
                expansion.properties.insert(lower_id, infer_property(&release.value, None)?);
                expansion.releases.insert(lower_id, release);
                component.insert_argument(&"lower".into(), lower_id);
            }

            if let Some(upper_id) = upper_id {
                let (patch_node, release) = get_literal(Value::Array(data_property.upper()?), component.submission)?;
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

impl Mechanism for proto::SnappingMechanism {
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


impl Accuracy for proto::SnappingMechanism {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        accuracies: &proto::Accuracies,
        _public_arguments: IndexMap<base::IndexKey, &Value>
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
                    epsilon: accuracy_to_epsilon(accuracy.value, accuracy.alpha, *sensitivity),
                    delta: 0.,
                }))
            })
            .collect()))
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        mut public_arguments: IndexMap<base::IndexKey, &Value>,
        alpha: f64
    ) -> Result<Option<Vec<proto::Accuracy>>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator.as_ref()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // sensitivity must be computable
        let sensitivities = sensitivity_values.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let lower = standardize_numeric_argument(
            public_arguments.remove(&IndexKey::from("lower"))
                .ok_or_else(|| Error::from("lower: missing"))?.clone().array()?.float()?,
            data_property.num_columns()?)?
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();

        let upper = standardize_numeric_argument(
            public_arguments.remove(&IndexKey::from("upper"))
                .ok_or_else(|| Error::from("upper: missing"))?.clone().array()?.float()?,
            data_property.num_columns()?)?
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();

        Ok(Some(sensitivities.into_iter().zip(epsilons.into_iter())
            .zip(lower.into_iter().zip(upper.into_iter()))
            .map(|((sensitivity, epsilon), (lower, upper))| proto::Accuracy {
                value: epsilon_to_accuracy(alpha, epsilon, *sensitivity, (upper - lower) / 2.),
                alpha,
            })
            .collect()))
    }
}

/// Finds the smallest integer m such that 2^m is equal to or greater than x.
///
/// # Arguments
/// * `x` - The number for which we want the next power of two.
///
/// # Returns
/// The found power of two
pub fn get_smallest_greater_or_eq_power_of_two(x: f64) -> i16 {
    x.log2().ceil() as i16
}

/// Gets functional epsilon for Snapping mechanism such that privacy loss does not exceed the user's proposed budget.
/// Described in https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `b` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Functional epsilon that will determine amount of noise.
pub fn redefine_epsilon(epsilon: f64, b: f64, precision: u32) -> f64 {
    let eta = 2_f64.powi(-(precision as i32));
    (epsilon - 2.0 * eta) / (1.0 + 12.0 * b * eta)
}

/// Finds accuracy that is achievable given desired epsilon and confidence requirements. Described in
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `alpha` - Desired confidence level.
/// * `epsilon` - Desired privacy guarantee.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `b` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn epsilon_to_accuracy(
    alpha: f64, epsilon: f64, sensitivity: f64, b: f64
) -> f64 {
    let precision = compute_precision(epsilon);
    let epsilon = redefine_epsilon(epsilon, b, precision);
    let lambda = 2f64.powi(get_smallest_greater_or_eq_power_of_two(1.0 / epsilon) as i32); // 2^m
    ((1.0 / alpha).ln() / epsilon + lambda / 2.) * sensitivity
}

/// Finds epsilon that will achieve desired accuracy and confidence requirements. Described in
/// https://github.com/ctcovington/floating_point/blob/master/snapping_mechanism/notes/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `accuracy` - Desired accuracy level.
/// * `alpha` - Desired confidence level.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
///
/// # Returns
/// Epsilon use for the Snapping mechanism.
pub fn accuracy_to_epsilon(
    accuracy: f64, alpha: f64, sensitivity: f64
) -> f64 {
    (1. - alpha.ln()) / accuracy * sensitivity
}


/// Finds the necessary precision for the snapping mechanism
/// 118 bits required for LN
/// Floating-point-exponent + 2 bits required for non-zero epsilon
/// # Arguments
/// * `epsilon` - privacy usage before redefinition
pub fn compute_precision(epsilon: f64) -> u32 {
    118.max(get_smallest_greater_or_eq_power_of_two(epsilon) + 2) as u32
}






#[cfg(test)]
pub mod test_get_smallest_greater_or_eq_power_of_two {
    use crate::components::snapping_mechanism::get_smallest_greater_or_eq_power_of_two;

    pub fn ieee754_crate_get_smallest_geq_pow2(x: f64) -> i16 {
        use ieee754::Ieee754;
        let (_sign, exponent, mantissa) = x.decompose();
        exponent + if mantissa == 0 {0} else {1}
    }

    #[test]
    fn test() {
        (1..1000)
            .map(|i| i as f64 / 100.)
            .for_each(|v| assert_eq!(
                get_smallest_greater_or_eq_power_of_two(v),
                ieee754_crate_get_smallest_geq_pow2(v)))
    }
}