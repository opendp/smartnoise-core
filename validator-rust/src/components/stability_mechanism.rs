use crate::errors::*;

use crate::{proto, base, Warnable, Float};

use crate::components::{Component, Expandable};
use crate::base::{Value, ValueProperties, DataType, IndexKey, DataframeProperties, ArrayProperties, NodeProperties, SensitivitySpace, NatureContinuous, Vector1DNull};
use crate::utilities::{prepend, expand_mechanism, get_literal};
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;
use crate::utilities::privacy::spread_privacy_usage;


impl Component for proto::StabilityMechanism {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let data_property: &DataframeProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.dataframe()
            .map_err(prepend("data:"))?;

        let mut counts_property: ArrayProperties = data_property.children
            .get::<IndexKey>(&"counts".into())
            .ok_or_else(|| Error::from("data: counts must be a column in the dataframe"))?
            .array()?.clone();

        let mut categories_property: ArrayProperties = data_property.children
            .get::<IndexKey>(&"categories".into())
            .ok_or_else(|| Error::from("data: categories must be a column in the dataframe"))?
            .array()?.clone();

        if counts_property.dataset_id != categories_property.dataset_id {
            return Err("counts and categories must come from the same dataset".into());
        }

        if counts_property.data_type != DataType::Int {
            return Err("data: counts atomic type must be integral".into());
        }

        let mut mechanism = self.mechanism.to_lowercase();
        if mechanism == "automatic" { mechanism = "simplegeometric".to_string() }
        counts_property.data_type = match mechanism.as_str() {
            "simplegeometric" => DataType::Int,
            "gaussian" => DataType::Float,
            "laplace" => DataType::Float,
            _ => return Err(format!("mechanism: {:?} not recognized", self.mechanism).into())
        };

        let aggregator = counts_property.aggregator.as_ref()
            .ok_or_else(|| Error::from("data: must be aggregated"))?;

        if !aggregator.censor_rows {
            return Err("the stability mechanism is only intended to be used for data that needs censoring".into())
        }

        if !matches!(aggregator.component, proto::component::Variant::Histogram(_)) {
            return Err("data must be aggregated by a histogram".into());
        }

        counts_property.releasable = true;
        categories_property.releasable = true;

        Ok(ValueProperties::Dataframe(DataframeProperties {
            children: indexmap![
                "categories".into() => ValueProperties::Array(categories_property),
                "counts".into() => ValueProperties::Array(counts_property),
            ]
        }).into())
    }
}

impl Expandable for proto::StabilityMechanism {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let data_property: &DataframeProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.dataframe()
            .map_err(prepend("data:"))?;

        // use the counts as the data argument to the mechanism expansion
        let counts_property: ValueProperties = data_property.children
            .get::<IndexKey>(&"counts".into())
            .ok_or_else(|| Error::from("data: counts must be a column in the dataframe"))?
            .clone();

        let mut mechanism = self.mechanism.to_lowercase();
        if mechanism == "automatic" { mechanism = "simplegeometric".to_string() }

        let sensitivity_space = SensitivitySpace::KNorm(match mechanism.as_str() {
            "automatic" => 1,
            "simplegeometric" => 1,
            "laplace" => 1,
            "gaussian" => 2,
            _ => return Err(format!("mechanism: {:?} not recognized", self.mechanism).into())
        });

        let mut expansion = base::ComponentExpansion::default();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy definition must be defined")?;

        // always overwrite sensitivity. This is not something a user may configure
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let aggregator = data_property.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity scaling
        let mut sensitivity_value = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &sensitivity_type)?;

        if aggregator.lipschitz_constants.array()?.float()?.iter().any(|v| v != &1.) {
            // TODO: this could be relaxed
            return Err("lipschitz constants must be 1. for the stability mechanism".into())
        }

        maximum_id += 1;
        let id_sensitivity = maximum_id;
        let (patch_node, release) = get_literal(sensitivity_value.clone(), component.submission)?;
        expansion.computation_graph.insert(id_sensitivity, patch_node);
        expansion.properties.insert(id_sensitivity, infer_property(&release.value, None)?);
        expansion.releases.insert(id_sensitivity, release);

        // spread privacy usage over each column
        let spread_usages = spread_privacy_usage(
            // spread usage over each column
            privacy_usage, sensitivity_value.array()?.num_columns()? as usize)?;

        // convert to effective usage
        let effective_usages = spread_usages.into_iter()
            .zip(data_property.c_stability.iter())
            // reduce epsilon allowed to algorithm based on c-stability and group size
            .map(|(usage, c_stab)|
                usage.actual_to_effective(1., *c_stab as f64, privacy_definition.group_size))
            .collect::<Result<Vec<proto::PrivacyUsage>>>()?;

        // insert sensitivity and usage
        let mut noise_component = component.clone();
        noise_component.insert_argument(&"sensitivity".into(), id_sensitivity);

        let mut variant = self.clone();
        variant.privacy_usage = effective_usages;

        expansion.computation_graph.insert(component_id, noise_component);

        Ok(expansion.into())
    }
}


impl Component for proto::TauThreshold {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        if data_property.data_type != DataType::Float && data_property.data_type != DataType::Int {
            return Err("data: atomic type must be numeric".into())
        }

        let tau_threshold = compute_tau_threshold()?;

        data_property.num_records = Some(1);
        data_property.nature = Some(NatureContinuous {
            lower: Vector1DNull::Float(vec![Some(tau_threshold)]),
            upper: Vector1DNull::Float(vec![None])
        });
        data_property.dimensionality = Some(1);
        data_property.dataset_id = Some(node_id as i64);

        Ok(ValueProperties::Array(data_property).into())
    }
}

fn compute_tau_threshold() -> Result<Float> {
    Ok(1.)
}