use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, NodeProperties, ArrayND, ValueProperties, NatureCategorical};

use crate::{proto, base};
use crate::utilities::{prepend, get_literal};
use crate::components::{Component, Expandable};

use ndarray::Array;
use crate::base::{Value, NatureContinuous};


impl Component for proto::Clamp {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns
            .ok_or("data: number of data columns missing")?;

        // handle categorical clamping
        if let Some(categories) = public_arguments.get("categories") {
            data_property.nature = Some(Nature::Categorical(NatureCategorical {
                categories: categories.get_jagged()?.to_owned()
            }));

            return Ok(data_property.into());
        }

        // 1. check public arguments (constant n)
        let mut clamp_minimum = match public_arguments.get("min") {
            Some(min) => min.clone().get_arraynd()?.clone().get_vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("min") {
                Some(min) => min.get_arraynd()?.get_min_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .get_min_f64()?
            }
        };

        // 1. check public arguments (constant n)
        let mut clamp_maximum = match public_arguments.get("max") {
            Some(max) => max.get_arraynd()?.clone().get_vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("max") {
                Some(min) => min.get_arraynd()?.get_max_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .get_max_f64()?
            }
        };

        if !clamp_minimum.iter().zip(clamp_maximum.clone()).all(|(min, max)| *min < max) {
            return Err("minimum is greater than maximum".into());
        }

        // the actual data bound (if it exists) may be tighter than the clamping parameters
        if let Ok(data_minimum) = data_property.get_min_f64_option() {
            clamp_minimum = clamp_minimum.iter().zip(data_minimum)
                // match on if the actual bound exists for each column, and remain conservative if not
                .map(|(clamp_min, optional_data_min)| match optional_data_min {
                    Some(data_min) => clamp_min.max(data_min), // tighter data bound is only applied here
                    None => clamp_min.clone()
                }).collect()
        }
        if let Ok(data_maximum) = data_property.get_max_f64_option() {
            clamp_maximum = clamp_maximum.iter().zip(data_maximum)
                .map(|(clamp_max, optional_data_max)| match optional_data_max {
                    Some(data_max) => clamp_max.min(data_max),
                    None => clamp_max.clone()
                }).collect()
        }

        // save revised bounds
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(clamp_minimum.iter().map(|x| Some(x.clone())).collect()),
            max: Vector1DNull::F64(clamp_maximum.iter().map(|x| Some(x.clone())).collect()),
        }));

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::Clamp {
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
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_arraynd()?.get_min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_arraynd()?.get_max_f64()?).into_dyn()));
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