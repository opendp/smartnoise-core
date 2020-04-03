use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;

use crate::components::{Component, Expandable};
use ndarray;

use crate::base::{Value, Array, Nature, NatureContinuous, Vector1DNull, ValueProperties, DataType, NatureCategorical};
use crate::utilities::{prepend, get_literal};


impl Component for proto::Resize {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        data_property.assert_is_not_aggregated()?;

        let num_columns = data_property.num_columns()?;

        let num_records = public_arguments.get("n")
            .ok_or("n must be passed to Resize")?.first_i64()?;

        if num_records < 1 {
            return Err("n must be greater than zero".into())
        }

        if let Some(categories) = public_arguments.get("categories") {
            data_property.nature = Some(Nature::Categorical(NatureCategorical {
                categories: categories.jagged()?.standardize(&num_columns)?
            }));
            data_property.num_records = Some(num_records);
            return Ok(data_property.into());
        }

        match data_property.data_type {
            DataType::F64 => {

                // 1. check public arguments (constant n)
                let impute_minimum = match public_arguments.get("min") {
                    Some(min) => min.array()?.clone().vec_f64(Some(num_columns))
                        .map_err(prepend("min:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("min") {
                        Some(min) => min.array()?.min_f64()
                            .map_err(prepend("min:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .min_f64().map_err(prepend("min:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_maximum = match public_arguments.get("max") {
                    Some(max) => max.array()?.clone().vec_f64(Some(num_columns))
                        .map_err(prepend("max:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("max") {
                        Some(min) => min.array()?.max_f64()
                            .map_err(prepend("max:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .max_f64().map_err(prepend("max:"))?
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

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    min: Vector1DNull::F64(impute_minimum),
                    max: Vector1DNull::F64(impute_maximum),
                }));
            },

            DataType::I64 => {

                // 1. check public arguments (constant n)
                let impute_minimum = match public_arguments.get("min") {
                    Some(min) => min.array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("min:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("min") {
                        Some(min) => min.array()?.min_i64()
                            .map_err(prepend("min:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .min_i64().map_err(prepend("min:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_maximum = match public_arguments.get("max") {
                    Some(max) => max.array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("max:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("max") {
                        Some(min) => min.array()?.max_i64()
                            .map_err(prepend("max:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .max_i64().map_err(prepend("max:"))?
                    }
                };

                if !impute_minimum.iter().zip(impute_maximum.clone()).all(|(min, max)| *min < max) {
                    return Err("minimum is greater than maximum".into());
                }

                // the actual data bound (if it exists) may be wider than the imputation parameters
                let impute_minimum = match data_property.min_i64_option() {
                    Ok(data_minimum) => impute_minimum.into_iter().zip(data_minimum.into_iter())
                        .map(|(impute_min, optional_data_min)| match optional_data_min {
                            Some(data_min) => Some(impute_min.min(data_min)),
                            // since there was no prior bound, nothing is known about the min
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                let impute_maximum = match data_property.max_i64_option() {
                    Ok(data_maximum) => impute_maximum.into_iter().zip(data_maximum.into_iter())
                        .map(|(impute_max, optional_data_max)| match optional_data_max {
                            Some(data_max) => Some(impute_max.max(data_max)),
                            // since there was no prior bound, nothing is known about the max
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    min: Vector1DNull::I64(impute_minimum),
                    max: Vector1DNull::I64(impute_maximum),
                }));
            }
            _ => return Err("bounds for imputation must be numeric".into())
        }

        data_property.num_records = Some(num_records);
        Ok(data_property.into())
    }


}

impl Expandable for proto::Resize {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id;
            let value = Value::Array(Array::F64(
                ndarray::Array::from(data_property.min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id;
            let value = Value::Array(Array::F64(
                ndarray::Array::from(data_property.max_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_max.clone(), patch_node);
            releases.insert(id_max.clone(), release);
            component.arguments.insert("max".to_string(), id_max);
        }

        if !properties.contains_key("n") {
            current_id += 1;
            let id_n = current_id;
            let value = Value::Array(Array::I64(ndarray::Array::from_shape_vec(
                (), vec![data_property.num_records()?])
                .unwrap().into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_n.clone(), patch_node);
            releases.insert(id_n.clone(), release);
            component.arguments.insert("n".to_string(), id_n);
        }

        computation_graph.insert(*component_id, component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}