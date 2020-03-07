use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;

use crate::components::{Component, Expandable};
use ndarray::Array;

use crate::base::{Value, Properties, ArrayND, NodeProperties, get_constant, Nature, NatureContinuous, Vector1DNull};


impl Component for proto::Resize {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data must be passed to Resize")?.clone();

        let num_columns = data_property.num_columns
            .ok_or("num_columns must be passed to Resize")?;

        let num_records = public_arguments.get("n")
            .ok_or("n must be passed to Resize")?.clone().get_arraynd()?.get_i64()?;

        if num_records.len() as i64 > 1 {
            Err("n must be a scalar")?;
        }
        let num_records: i64 = match num_records.first() {
            Some(first) => first.to_owned(),
            None => return Err("n cannot be none".into())
        };

        // 1. check public arguments (constant n)
        let impute_minimum = match public_arguments.get("min") {
            Some(min) => min.clone().get_arraynd()?.get_vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("min") {
                Some(min) => min.get_min_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .get_min_f64()?
            }
        };

        // 1. check public arguments (constant n)
        let impute_maximum = match public_arguments.get("max") {
            Some(max) => max.clone().get_arraynd()?.get_vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("max") {
                Some(min) => min.get_max_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .get_max_f64()?
            }
        };

        if !impute_minimum.iter().zip(impute_maximum.clone()).all(|(min, max)| *min < max) {
            return Err("minimum is greater than maximum".into());
        }

        // the actual data bound (if it exists) may be wider than the imputation parameters
        let impute_minimum = match data_property.get_min_f64_option() {
            Ok(data_minimum) => impute_minimum.iter().zip(data_minimum)
                .map(|(impute_min, optional_data_min)| match optional_data_min {
                    Some(data_min) => Some(impute_min.min(data_min)),
                    // since there was no prior bound, nothing is known about the min
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        let impute_maximum = match data_property.get_max_f64_option() {
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

        data_property.num_records = (0..num_columns)
            .map(|_x| Some(num_records.clone()))
            .collect::<Vec<Option<i64>>>();

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::Resize {
    fn expand_graph(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id;
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_min_f64()?).into_dyn()));
            graph_expansion.insert(id_min.clone(), get_constant(&value, &component.batch));
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_max_f64()?).into_dyn()));
            graph_expansion.insert(id_max, get_constant(&value, &component.batch));
            component.arguments.insert("max".to_string(), id_max);
        }

        if !properties.contains_key("n") {
            current_id += 1;
            let id_n = current_id.clone();
            let value = Value::ArrayND(ArrayND::I64(Array::from_shape_vec(
                (), properties.get("data").unwrap().to_owned().get_n()?)
                .unwrap().into_dyn()));

            graph_expansion.insert(id_n, get_constant(&value, &component.batch));
            component.arguments.insert("n".to_string(), id_n);
        }

        graph_expansion.insert(component_id, component);
        Ok((current_id, graph_expansion))
    }
}