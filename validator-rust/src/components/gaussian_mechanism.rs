use ::itertools::izip;
use indexmap::map::IndexMap;
use itertools::Itertools;
use statrs::function::erf;

use crate::{base, proto, Warnable};
use crate::base::{DataType, IndexKey, NodeProperties, SensitivitySpace, Value, ValueProperties};
use crate::components::{Accuracy, Mechanism, Sensitivity};
use crate::components::{Component, Expandable};
use crate::errors::*;
use crate::utilities::{expand_mechanism, prepend};
use crate::utilities::privacy::{get_delta, get_epsilon, privacy_usage_check, spread_privacy_usage};

impl Component for proto::GaussianMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.protect_floating_point {
            return Err("Floating-point protections are enabled. The gaussian mechanism is susceptible to floating-point attacks.".into())
        }

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into());
        }

        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::Float && data_property.data_type != DataType::Int {
            return Err("data: atomic type must be numeric".into());
        }
        let aggregator = data_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(2))?.array()?.cast_float()?;

        // make sure lipschitz constants are available as float arrays
        aggregator.lipschitz_constants.array()?.cast_float()?;

        let privacy_usage = self.privacy_usage.iter().cloned().map(Ok)
            .fold1(|l, r| l? + r?).ok_or_else(|| "privacy_usage: must be defined")??;

        let warnings = privacy_usage_check(
            &privacy_usage,
            data_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        let epsilon = get_epsilon(&privacy_usage)?;
        if !self.analytic && epsilon > 1.0 {
            let message = Error::from(format!(
                "Warning: A privacy parameter of epsilon = {} is in use. \
                Privacy is only guaranteed for the Gaussian mechanism for epsilon between 0 and 1. \
                Use the 'AnalyticGaussian' instead.", epsilon));

            return Err(message)
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
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        expand_mechanism(
            &SensitivitySpace::KNorm(2),
            privacy_definition,
            self.privacy_usage.as_ref(),
            component,
            properties,
            component_id,
            maximum_id,
        )
    }
}

impl Mechanism for proto::GaussianMechanism {
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
            .map(|usage| usage.effective_to_actual(
                data_property.sample_proportion.unwrap_or(1.),
                data_property.c_stability,
                privacy_definition.group_size))
            .collect::<Result<Vec<proto::PrivacyUsage>>>()).transpose()
    }
}


impl Accuracy for proto::GaussianMechanism {
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

        let aggregator = data_property.aggregator.as_ref()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(2))?;

        // take max sensitivity of each column
        let sensitivities: Vec<_> = sensitivity_values.array()?.cast_float()?
            .gencolumns().into_iter()
            .map(|sensitivity_col| sensitivity_col.into_iter().copied().fold1(|l, r| l.max(r)).unwrap())
            .collect();

        let usages = spread_privacy_usage(&self.privacy_usage, data_property.num_columns()? as usize)?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;
        let iter = izip!(sensitivities.into_iter(), accuracies.values.iter(), delta.into_iter());

        use proto::privacy_usage::{Distance, DistanceApproximate};

        Some(iter.map(|(sensitivity, accuracy, delta)| {
            let sigma: f64 = if self.analytic {
                return Err(Error::from("converting to privacy usage is not implemented for the analytic gaussian"))
            } else {
                (2.0 * (1.25 / delta).ln()).sqrt() * sensitivity as f64 / accuracy.value
            };

            Ok(proto::PrivacyUsage {
                distance: Some(Distance::Approximate(DistanceApproximate {
                    epsilon: sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - accuracy.alpha),
                    delta,
                }))
            })
        }).collect()).transpose()
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &base::NodeProperties,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        alpha: f64,
    ) -> Result<Option<Vec<proto::Accuracy>>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let sensitivity_values = aggregator.component.compute_sensitivity(
            &privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::KNorm(1))?;

        // take max sensitivity of each column
        let sensitivities: Vec<_> = sensitivity_values.array()?.cast_float()?
            .gencolumns().into_iter()
            .map(|sensitivity_col| sensitivity_col.into_iter().copied().fold1(|l, r| l.max(r)).unwrap())
            .collect();

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivities.len())?;
        let epsilons = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let deltas = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;
        let iter = izip!(sensitivities.into_iter(), epsilons.into_iter(), deltas.into_iter());

        Ok(Some(iter.map(|(sensitivity, epsilon, delta)| {
            let sigma: f64 = if self.analytic {
                get_analytic_gaussian_sigma(epsilon, delta, sensitivity as f64)
            } else {
                sensitivity as f64 * (2.0 * (1.25 / delta).ln()).sqrt() / epsilon
            };

            proto::Accuracy {
                value: sigma * 2.0_f64.sqrt() * erf::erf_inv(1.0_f64 - alpha),
                alpha,
            }
        }).collect()))
    }
}

fn phi(t: f64) -> f64 {
    0.5 * (1. + erf::erf(t / 2.0_f64.sqrt()))
}

fn case_a(epsilon: f64, s: f64) -> f64 {
    phi((epsilon * s).sqrt()) - epsilon.exp() * phi(-(epsilon * (s + 2.)).sqrt())
}

fn case_b(epsilon: f64, s: f64) -> f64 {
    phi(-(epsilon * s).sqrt()) - epsilon.exp() * phi(-(epsilon * (s + 2.)).sqrt())
}

fn doubling_trick(
    mut s_inf: f64, mut s_sup: f64, epsilon: f64, delta: f64, delta_thr: f64,
) -> (f64, f64) {
    let predicate = |s: f64| if delta > delta_thr {
        case_a(epsilon, s) < delta
    } else {
        case_b(epsilon, s) > delta
    };

    while predicate(s_sup) {
        s_inf = s_sup;
        s_sup = 2.0 * s_inf;
    }
    (s_inf, s_sup)
}

fn binary_search(
    mut s_inf: f64, mut s_sup: f64, epsilon: f64, delta: f64, delta_thr: f64, tol: f64,
) -> f64 {
    let mut s_mid: f64 = s_inf + (s_sup - s_inf) / 2.;

    let s_to_delta = |s: f64| if delta > delta_thr {
        case_a(epsilon, s)
    } else {
        case_b(epsilon, s)
    };

    loop {
        let delta_prime = s_to_delta(s_mid);

        let diff = delta_prime - delta;
        if (diff.abs() <= tol) && (diff <= 0.) { break }

        let is_left = if delta > delta_thr {
            delta_prime > delta
        } else {
            delta_prime < delta
        };

        if is_left {
            s_sup = s_mid;
        } else {
            s_inf = s_mid;
        }
        s_mid = s_inf + (s_sup - s_inf) / 2.;
    }
    s_mid
}

/// Compute the sigma to parameterize a gaussian distribution
pub fn get_analytic_gaussian_sigma(epsilon: f64, delta: f64, sensitivity: f64) -> f64 {
    let delta_thr = case_a(epsilon, 0.);

    let alpha = if delta == delta_thr {
        1.
    } else {
        let (s_inf, s_sup) = doubling_trick(0., 1., epsilon, delta, delta_thr);
        let tol: f64 = 1e-10f64;
        let s_final = binary_search(s_inf, s_sup, epsilon, delta, delta_thr, tol);
        let sign = if delta >= delta_thr { -1. } else { 1. };
        (1. + s_final / 2.).sqrt() + sign * (s_final / 2.).sqrt()
    };

    alpha * sensitivity / (2. * epsilon).sqrt()
}

#[cfg(test)]
mod test_analytic_gaussian {
    use crate::components::gaussian_mechanism::get_analytic_gaussian_sigma;

    #[test]
    fn test_analytic_gaussian_sigma() {
        println!("{:?}", get_analytic_gaussian_sigma(0.5, 1E-10, 1.))
    }
}