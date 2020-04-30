use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;
use crate::components::{Component, Expandable};

use ndarray;
use crate::base::{Vector1DNull, Nature, NatureContinuous, Value, Array, ValueProperties, DataType};
use crate::utilities::{prepend, get_literal};


impl Component for proto::Impute {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        if data_property.data_type == DataType::I64 {
            return Ok(data_property.into())
        }

        if let Some(_categories) = public_arguments.get("categories") {
            // TODO: propagation of categories through imputation and resize
            data_property.nature = None;
            return Ok(data_property.into());
        }

        let num_columns = data_property.num_columns
            .ok_or("data: number of columns missing")?;
        // 1. check public arguments (constant n)
        let impute_lower = match public_arguments.get("lower") {
            Some(min) => min.array()?.clone().vec_f64(Some(num_columns))
                .map_err(prepend("lower:"))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("lower") {
                Some(min) => min.array()?.lower_f64()
                    .map_err(prepend("lower:"))?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .lower_f64().map_err(prepend("lower:"))?
            }
        };

        // 1. check public arguments (constant n)
        let impute_upper = match public_arguments.get("upper") {
            Some(max) => max.array()?.clone().vec_f64(Some(num_columns))
                .map_err(prepend("upper:"))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("upper") {
                Some(min) => min.array()?.upper_f64()
                    .map_err(prepend("max:"))?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .upper_f64().map_err(prepend("upper:"))?
            }
        };

        if !impute_lower.iter().zip(impute_upper.clone()).all(|(low, high)| *low < high) {
            return Err("lower is greater than upper".into());
        }

        // the actual data bound (if it exists) may be wider than the imputation parameters
        let impute_lower = match data_property.lower_f64_option() {
            Ok(data_lower) => impute_lower.iter().zip(data_lower)
                .map(|(impute_lower, optional_data_lower)| match optional_data_lower {
                    Some(data_lower) => Some(impute_lower.min(data_lower)),
                    // since there was no prior bound, nothing is known about the min
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        let impute_upper = match data_property.upper_f64_option() {
            Ok(data_upper) => impute_upper.iter().zip(data_upper)
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
            lower: Vector1DNull::F64(impute_lower),
            upper: Vector1DNull::F64(impute_upper),
        }));

        Ok(data_property.into())
    }


}

impl Expandable for proto::Impute {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("categories") {
            if !properties.contains_key("lower") {
                current_id += 1;
                let id_lower = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.lower_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(&value, &component.batch)?;
                computation_graph.insert(id_lower.clone(), patch_node);
                releases.insert(id_lower.clone(), release);
                component.arguments.insert("lower".to_string(), id_lower);
            }

            if !properties.contains_key("upper") {
                current_id += 1;
                let id_upper = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.upper_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(&value, &component.batch)?;
                computation_graph.insert(id_upper.clone(), patch_node);
                releases.insert(id_upper.clone(), release);
                component.arguments.insert("upper".to_string(), id_upper);
            }
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