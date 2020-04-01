use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;
use crate::components::{Component, Expandable};

use ndarray;
use crate::base::{Vector1DNull, Nature, NatureContinuous, Value, NodeProperties, Array, ValueProperties};
use crate::utilities::{prepend, get_literal};


impl Component for proto::Impute {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns
            .ok_or("data: number of columns missing")?;
        // 1. check public arguments (constant n)
        let impute_minimum = match public_arguments.get("min") {
            Some(min) => min.array()?.clone().vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("min") {
                Some(min) => min.array()?.min_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .min_f64()?
            }
        };

        // 1. check public arguments (constant n)
        let impute_maximum = match public_arguments.get("max") {
            Some(max) => max.array()?.clone().vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("max") {
                Some(min) => min.array()?.max_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .max_f64()?
            }
        };

        if !impute_minimum.iter().zip(impute_maximum.clone()).all(|(min, max)| *min < max) {
            return Err("minimum is greater than maximum".into());
        }

        // the actual data bound (if it exists) may be wider than the imputation parameters
        let impute_minimum = match data_property.min_f64_option() {
            Ok(data_minimum) => impute_minimum.iter().zip(data_minimum)
                .map(|(impute_min, optional_data_min)| match optional_data_min {
                    Some(data_min) => Some(impute_min.min(data_min)),
                    // since there was no prior bound, nothing is known about the min
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        let impute_maximum = match data_property.max_f64_option() {
            Ok(data_maximum) => impute_maximum.iter().zip(data_maximum)
                .map(|(impute_max, optional_data_max)| match optional_data_max {
                    Some(data_max) => Some(impute_max.max(data_max)),
                    // since there was no prior bound, nothing is known about the max
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        data_property.nullity = false;

        // impute may only ever widen prior existing bounds
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(impute_minimum),
            max: Vector1DNull::F64(impute_maximum),
        }));

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        arg_vars: HashMap<String, Vec<String>>,
    ) -> Result<Vec<String>> {
        return Ok(arg_vars.values().cloned().flatten().collect::<Vec<String>>());
    }
}

impl Expandable for proto::Impute {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = maximum_id.clone();
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.max_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_max.clone(), patch_node);
            releases.insert(id_max.clone(), release);
            component.arguments.insert("max".to_string(), id_max);
        }

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}