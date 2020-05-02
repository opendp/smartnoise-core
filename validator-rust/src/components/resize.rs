use crate::errors::*;


use std::collections::HashMap;

use crate::base;
use crate::proto;

use crate::components::{Component, Expandable};
use ndarray;

use crate::base::{Value, Array, Nature, NatureContinuous, Vector1DNull, ValueProperties, DataType};
use crate::utilities::{prepend, get_literal};


impl Component for proto::Resize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        if let Some(num_columns) = public_arguments.get("number_columns") {
            let num_columns = num_columns.first_i64()?;
            if num_columns < 1 {
                return Err("n must be greater than zero".into());
            }
            data_property.num_columns = Some(num_columns);
        }

        if let Some(num_records) = public_arguments.get("number_rows") {
            let num_records = num_records.first_i64()?;
            if num_records < 1 {
                return Err("n must be greater than zero".into());
            }
            data_property.num_records = Some(num_records);
            data_property.is_not_empty = num_records > 0;
        }

        if let Some(categories) = public_arguments.get("categories") {
            if data_property.data_type != categories.jagged()?.data_type() {
                return Err("data's data_type must match categories' data_type".into());
            }
            // TODO: propagation of categories through imputation and resize
            data_property.nature = None;
            return Ok(data_property.into());
        }

        match data_property.data_type {
            DataType::F64 => {

                // 1. check public arguments (constant n)
                let impute_lower = match public_arguments.get("lower") {
                    Some(lower) => lower.array()?.clone().vec_f64(Some(num_columns))
                        .map_err(prepend("lower:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("lower") {
                        Some(lower) => lower.array()?.lower_f64()
                            .map_err(prepend("lower:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .lower_f64().map_err(prepend("min:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_upper = match public_arguments.get("upper") {
                    Some(upper) => upper.array()?.clone().vec_f64(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("upper") {
                        Some(upper) => upper.array()?.upper_f64()
                            .map_err(prepend("upper:"))?,

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
                        .map(|(impute_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => Some(impute_upper.max(data_upper)),
                            // since there was no prior bound, nothing is known about the max
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::F64(impute_lower),
                    upper: Vector1DNull::F64(impute_upper),
                }));
            }

            DataType::I64 => {

                // 1. check public arguments (constant n)
                let impute_lower = match public_arguments.get("lower") {
                    Some(lower) => lower.array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("lower:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("lower") {
                        Some(lower) => lower.array()?.lower_i64()
                            .map_err(prepend("lower:"))?,

                        // 3. then data properties (propagated from prior clamping/lower/upper)
                        None => data_property
                            .lower_i64().map_err(prepend("lower:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_upper = match public_arguments.get("upper") {
                    Some(upper) => upper.array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("upper") {
                        Some(upper) => upper.array()?.upper_i64()
                            .map_err(prepend("upper:"))?,

                        // 3. then data properties (propagated from prior clamping/lower/upper)
                        None => data_property
                            .upper_i64().map_err(prepend("upper:"))?
                    }
                };

                if !impute_lower.iter().zip(impute_upper.clone()).all(|(low, high)| *low < high) {
                    return Err("lower is greater than upper".into());
                }

                // the actual data bound (if it exists) may be wider than the imputation parameters
                let impute_lower = match data_property.lower_i64_option() {
                    Ok(data_lower) => impute_lower.into_iter().zip(data_lower.into_iter())
                        .map(|(impute_lower, optional_data_lower)| match optional_data_lower {
                            Some(data_lower) => Some(impute_lower.min(data_lower)),
                            // since there was no prior bound, nothing is known about the min
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                let impute_upper = match data_property.upper_i64_option() {
                    Ok(data_upper) => impute_upper.into_iter().zip(data_upper.into_iter())
                        .map(|(impute_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => Some(impute_upper.max(data_upper)),
                            // since there was no prior bound, nothing is known about the max
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::I64(impute_lower),
                    upper: Vector1DNull::I64(impute_upper),
                }));
            }
            _ => return Err("bounds for imputation must be numeric".into())
        }

        Ok(data_property.into())
    }
}

impl Expandable for proto::Resize {
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

        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !properties.contains_key("categories") {
            if !properties.contains_key("lower") {
                current_id += 1;
                let id_lower = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(data_property.lower_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(value, &component.batch)?;
                computation_graph.insert(id_lower.clone(), patch_node);
                releases.insert(id_lower.clone(), release);
                component.arguments.insert("lower".to_string(), id_lower);
            }

            if !properties.contains_key("upper") {
                current_id += 1;
                let id_upper = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(data_property.upper_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(value, &component.batch)?;
                computation_graph.insert(id_upper.clone(), patch_node);
                releases.insert(id_upper.clone(), release);
                component.arguments.insert("upper".to_string(), id_upper);
            }
        }

        computation_graph.insert(*component_id, component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new(),
        })
    }
}