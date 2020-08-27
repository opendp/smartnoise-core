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
use ieee754::Ieee754;
use std::cmp::Ordering;

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
            .zip(data_property.sample_proportion.iter())
            .map(|((usage, c_stab), s_prop)|
                usage.effective_to_actual(*s_prop, *c_stab as f64, privacy_definition.group_size))
            .collect::<Result<Vec<proto::PrivacyUsage>>>()).transpose()
    }
}


impl Accuracy for proto::SnappingMechanism {
    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        accuracies: &proto::Accuracies,
        mut public_arguments: IndexMap<base::IndexKey, &Value>
    ) -> Result<Option<Vec<proto::PrivacyUsage>>> {
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

        Some(sensitivities.into_iter().zip(accuracies.values.iter())
            .zip(lower.into_iter().zip(upper.into_iter()))
            .map(|((sensitivity, accuracy), (lower, upper))| Ok(proto::PrivacyUsage {
                distance: Some(proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: accuracy_to_epsilon(accuracy.value, accuracy.alpha, *sensitivity, (upper - lower) / 2.)?,
                    delta: 0.,
                }))
            }))
            .collect::<Result<Vec<_>>>()).transpose()
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

        Some(sensitivities.into_iter().zip(epsilons.into_iter())
            .zip(lower.into_iter().zip(upper.into_iter()))
            .map(|((sensitivity, epsilon), (lower, upper))| Ok(proto::Accuracy {
                value: epsilon_to_accuracy(alpha, epsilon, *sensitivity, (upper - lower) / 2.)?,
                alpha,
            }))
            .collect::<Result<Vec<_>>>()).transpose()
    }
}

/// Finds the smallest integer m such that 2^m is equal to or greater than x.
///
/// # Arguments
/// * `x` - The number for which we want the next power of two.
///
/// # Returns
/// The found power of two
pub fn get_smallest_greater_or_eq_power_of_two(x: f64) -> Result<i16> {
    if x <= 0. {
        return Err(Error::from("get_smallest_greater_or_equal_power_of_two must have a positive argument"))
    }
    let (_sign, exponent, mantissa) = x.decompose();
    Ok(exponent + if mantissa > 0 { 1 } else { 0 })
}

/// Gets functional epsilon for Snapping mechanism such that privacy loss does not exceed the user's proposed budget.
/// Described in https://github.com/opendifferentialprivacy/whitenoise-core/blob/develop/whitepapers/mechanisms/snapping/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `epsilon` - Desired privacy guarantee.
/// * `b` - Upper bound on function value being privatized.
/// * `precision` - Number of bits of precision to which arithmetic inside the mechanism has access.
///
/// # Returns
/// Functional epsilon that will determine amount of noise.
pub fn redefine_epsilon(epsilon: f64, b: f64, precision: u32) -> f64 {
    let eta = (-(precision as f64)).exp2();
    (epsilon - 2.0 * eta) / (1.0 + 12.0 * b * eta)
}

/// Finds accuracy that is achievable given desired epsilon and confidence requirements. Described in
/// https://github.com/opendifferentialprivacy/whitenoise-core/blob/develop/whitepapers/mechanisms/snapping/snapping_implementation_notes.pdf
///
/// # Arguments
/// * `alpha` - Desired confidence level.
/// * `epsilon` - Desired privacy guarantee.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
/// * `b` - Upper bound on function value being privatized.
///
/// # Returns
/// Accuracy of the Snapping mechanism.
#[allow(non_snake_case)]
pub fn epsilon_to_accuracy(
    alpha: f64, epsilon: f64, sensitivity: f64, b: f64
) -> Result<f64> {
    let precision = compute_precision(epsilon)?;
    let epsilon = redefine_epsilon(epsilon, b, precision);
    let Lambda = (get_smallest_greater_or_eq_power_of_two(1.0 / epsilon)? as f64).exp2(); // 2^m
    Ok((Lambda / 2. - alpha.ln() / epsilon) * sensitivity)
}

/// Finds epsilon that will achieve desired accuracy and confidence requirements. Described in
/// https://github.com/opendifferentialprivacy/whitenoise-core/blob/develop/whitepapers/mechanisms/snapping/snapping_implementation_notes.pdf
///
/// Note that not all accuracies have an epsilon, due to the clamping in the snapping mechanism.
/// In these cases, accuracy is treated as an upper bound,
///   and a larger epsilon is returned that guarantees a tighter accuracy.
///
/// # Arguments
/// * `accuracy` - Desired accuracy level (upper bound).
/// * `alpha` - Desired confidence level.
/// * `sensitivity` - l1 Sensitivity of function to which mechanism is being applied.
///
/// # Returns
/// Epsilon to use for the Snapping mechanism.
pub fn accuracy_to_epsilon(
    accuracy: f64, alpha: f64, sensitivity: f64, b: f64
) -> Result<f64> {

    // bounds for valid epsilon are derived in the whitepaper
    let mut eps_inf = 0.;
    let mut eps_sup = 1. / accuracy;

    let mut acc_prior = f64::NAN;
    let tol = 1e-20f64;

    loop {
        let eps_mid = eps_inf + (eps_sup - eps_inf) / 2.;
        let acc_candidate = epsilon_to_accuracy(alpha, eps_mid, sensitivity, b)?;

        match accuracy.partial_cmp(&acc_candidate) {
            Some(Ordering::Less) => eps_inf = eps_mid,
            Some(Ordering::Greater) => eps_sup = eps_mid,
            Some(Ordering::Equal) => return Ok(eps_mid),
            None => return Err(Error::from("non-comparable accuracy"))
        }

        let is_stuck= acc_prior == acc_candidate;
        let is_close = acc_candidate < accuracy && (accuracy - acc_candidate) <= tol;

        if is_close || is_stuck {
            return Ok(eps_sup)
        }
        acc_prior = acc_candidate;
    }
}


/// Finds the necessary precision for the snapping mechanism
/// 118 bits required for LN
/// -epsilon.log2().ceil() + 2 bits required for non-zero epsilon
///
/// # Arguments
/// * `epsilon` - privacy usage before redefinition
pub fn compute_precision(epsilon: f64) -> Result<u32> {
    Ok(118.max(get_smallest_greater_or_eq_power_of_two(epsilon)? + 2) as u32)
}
