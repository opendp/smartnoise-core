<<<<<<< HEAD
=======
use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component, Expandable, Sensitivity, Mechanism};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType, ArrayProperties, NodeProperties, IndexKey};
use crate::utilities::{prepend, get_literal};
use crate::utilities::privacy::{privacy_usage_check};
use itertools::Itertools;
>>>>>>> switch exponential mechanism from Value::Jagged -> Value::Array
use indexmap::map::IndexMap;
use itertools::Itertools;

use crate::{base, proto, Warnable};
use crate::base::{ArrayProperties, DataType, IndexKey, NodeProperties, SensitivitySpace, Value, ValueProperties};
use crate::components::{Component, Expandable, Mechanism, Sensitivity};
use crate::errors::*;
use crate::utilities::{get_argument, get_literal, prepend};
use crate::utilities::inference::infer_property;
use crate::utilities::privacy::privacy_usage_check;

impl Component for proto::ExponentialMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into());
        }

        let utilities_property = properties.get::<IndexKey>(&"utilities".into())
            .ok_or("utilities: missing")?.array()
            .map_err(prepend("utilities:"))?.clone();

        if utilities_property.data_type != DataType::Float {
            return Err("utilities: data_type must be float".into());
        }

        let candidates_property = properties.get(&IndexKey::from("candidates"))
            .ok_or_else(|| Error::from("candidates: missing"))?.array()?;

        if utilities_property.num_records()? != candidates_property.num_records()? {
            return Err("utilities and candidates must share the same number of records".into());
        }
        if utilities_property.num_columns()? != candidates_property.num_columns()? {
            return Err("utilities and candidates must share the same number of columns".into());
        }

        let aggregator = utilities_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // TODO: check that aggregator data id matches data id

        // sensitivity must be computable
        let sensitivity_values = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::Exponential)?;

        // make sure sensitivities are an f64 array
        sensitivity_values.array()?.float()?;

        let num_columns = utilities_property.num_columns()?;
        let output_property = ArrayProperties {
            num_records: Some(1),
            num_columns: Some(num_columns),
            nullity: false,
            releasable: true,
            c_stability: 1,
            aggregator: None,
            nature: None,
            data_type: candidates_property.data_type.clone(),
            dataset_id: None,
            is_not_empty: true,
<<<<<<< HEAD
            // TODO: preserve dimensionality through exponential mechanism
            //     All outputs become 2D, so 1D outputs are lost
            dimensionality: Some(2),
            group_id: vec![],
            naturally_ordered: true,
            sample_proportion: None
=======
            dimensionality: utilities_property.dimensionality.map(|v| v - 1),
            group_id: utilities_property.group_id
>>>>>>> switch exponential mechanism from Value::Jagged -> Value::Array
        };

        let privacy_usage = self.privacy_usage.iter().cloned().map(Ok)
            .fold1(|l, r| l? + r?)
            .ok_or_else(|| "privacy_usage: must be defined")??;

        let warnings = privacy_usage_check(
            &privacy_usage,
            output_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        Ok(Warnable(output_property.into(), warnings))
    }
}

impl Expandable for proto::ExponentialMechanism {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy definition must be defined")?;

        // always overwrite sensitivity. This is not something a user may configure
        let utilities_properties = properties.get::<IndexKey>(&"utilities".into())
            .ok_or("utilities: missing")?.array()
            .map_err(prepend("utilities:"))?.clone();

        let aggregator = utilities_properties.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::Exponential)?;

        maximum_id += 1;
        let id_sensitivity = maximum_id;
        let (patch_node, release) = get_literal(sensitivity, component.submission)?;
        expansion.computation_graph.insert(id_sensitivity, patch_node);
        expansion.properties.insert(id_sensitivity, infer_property(&release.value, None)?);
        expansion.releases.insert(id_sensitivity, release);

        // noising
        let mut noise_component = component.clone();
        noise_component.insert_argument(&"sensitivity".into(), id_sensitivity);

        expansion.computation_graph.insert(component_id, noise_component);

        Ok(expansion)
    }
}

impl Mechanism for proto::ExponentialMechanism {
    fn get_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        release_usage: Option<&Vec<proto::PrivacyUsage>>,
        properties: &NodeProperties,
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
