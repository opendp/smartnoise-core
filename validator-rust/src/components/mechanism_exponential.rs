use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base, Warnable};

use crate::components::{Component, Expandable, Sensitivity};
use crate::base::{Value, SensitivitySpace, ValueProperties, DataType, ArrayProperties};
use crate::utilities::{prepend, get_literal};
use crate::utilities::privacy::{broadcast_privacy_usage, get_epsilon, privacy_usage_reducer, privacy_usage_check};
use itertools::Itertools;

impl Component for proto::ExponentialMechanism {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into())
        }

        let utilities_property = properties.get("utilities")
            .ok_or("utilities: missing")?.jagged()
            .map_err(prepend("utilities:"))?.clone();

        if utilities_property.data_type != DataType::F64 {
            return Err("utilities: data_type must be float".into())
        }

        let candidates = public_arguments.get("candidates")
            .ok_or_else(|| Error::from("candidates: missing, must be public"))?.jagged()?;

        let utilities_num_records = utilities_property.num_records()?;
        let candidates_num_records = candidates.num_records();

        if utilities_num_records.len() != candidates_num_records.len() {
            return Err("utilities and candidates must share the same number of columns".into())
        }
        if !utilities_num_records.iter().zip(candidates_num_records.iter()).all(|(l, r)| l == r) {
            return Err("utilities and candidates must share the same number of rows in every column".into())
        }

        let aggregator = utilities_property.aggregator.clone()
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        // sensitivity must be computable
        let sensitivity_values = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::Exponential)?;

        // make sure sensitivities are an f64 array
        sensitivity_values.array()?.f64()?;

        let num_columns = utilities_property.num_columns()?;
        let mut output_property = ArrayProperties {
            num_records: Some(1),
            num_columns: Some(num_columns),
            nullity: false,
            releasable: true,
            c_stability: (0..num_columns).map(|_| 1.).collect(),
            aggregator: None,
            nature: None,
            data_type: candidates.data_type(),
            dataset_id: None,
            is_not_empty: true,
            // TODO: preserve dimensionality through exponential mechanism
            //     All outputs become 2D, so 1D outputs are lost
            dimensionality: Some(2)
        };

        let privacy_usage = self.privacy_usage.iter().cloned()
            .fold1(|l, r| privacy_usage_reducer(&l, &r, |l, r| l + r)).unwrap();

        let warnings = privacy_usage_check(
            &privacy_usage,
            output_property.num_records,
            privacy_definition.strict_parameter_checks)?;

        output_property.releasable = true;

        Ok(Warnable(output_property.into(), warnings))
    }
}


impl Expandable for proto::ExponentialMechanism {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy definition must be defined")?;
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        // always overwrite sensitivity. This is not something a user may configure
        let utilities_properties = properties.get("utilities")
            .ok_or("utilities: missing")?.jagged()
            .map_err(prepend("utilities:"))?.clone();

        let aggregator = utilities_properties.aggregator
            .ok_or_else(|| Error::from("aggregator: missing"))?;

        let sensitivity = aggregator.component.compute_sensitivity(
            privacy_definition,
            &aggregator.properties,
            &SensitivitySpace::Exponential)?;

        current_id += 1;
        let id_sensitivity = current_id;
        let (patch_node, release) = get_literal(sensitivity, &component.submission)?;
        computation_graph.insert(id_sensitivity.clone(), patch_node);
        releases.insert(id_sensitivity.clone(), release);

        // noising
        let mut noise_component = component.clone();
        noise_component.arguments.insert("sensitivity".to_string(), id_sensitivity);

        computation_graph.insert(component_id.clone(), noise_component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new(),
            warnings: vec![]
        })
    }
}
