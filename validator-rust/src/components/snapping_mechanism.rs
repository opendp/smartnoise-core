use indexmap::map::IndexMap;
use itertools::Itertools;

use crate::{base, proto, Warnable};
use crate::base::{DataType, IndexKey, NodeProperties, SensitivitySpace, Value, ValueProperties};
use crate::components::{Mechanism, Sensitivity};
use crate::components::{Component, Expandable};
use crate::errors::*;
use crate::utilities::{expand_mechanism, prepend, get_literal};
use crate::utilities::privacy::privacy_usage_check;
use crate::utilities::inference::infer_property;

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

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()?.clone();

        if let Some(lower_id) = lower_id {
            let lower_value: Value = ndarray::arr1(&data_property.lower_float()?).into_dyn().into();
            let (patch_node, release) = get_literal(lower_value, component.submission)?;
            expansion.computation_graph.insert(lower_id, patch_node);
            expansion.properties.insert(lower_id, infer_property(&release.value, None)?);
            expansion.releases.insert(lower_id, release);
        }

        if let Some(upper_id) = upper_id {
            let upper_value: Value = ndarray::arr1(&data_property.upper_float()?).into_dyn().into();
            let (patch_node, release) = get_literal(upper_value, component.submission)?;
            expansion.computation_graph.insert(upper_id, patch_node);
            expansion.properties.insert(upper_id, infer_property(&release.value, None)?);
            expansion.releases.insert(upper_id, release);
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
