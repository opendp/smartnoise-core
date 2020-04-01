use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, Array, ValueProperties, NatureCategorical, Jagged};

use crate::{proto, base};
use crate::utilities::{prepend, get_literal, standardize_null_target_argument};
use crate::components::{Component, Expandable};

use ndarray;
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
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns
            .ok_or("data: number of data columns missing")?;

        // handle categorical clamping
        if let Some(categories) = public_arguments.get("categories") {
            let null = public_arguments.get("null")
                .ok_or::<Error>("null value must be defined when clamping by categories".into())?
                .array()?;

            let mut categories = categories.jagged()?.clone();
            match (&mut categories, null) {
                (Jagged::F64(jagged), Array::F64(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.into_iter()
                            .for_each(|cats| cats.push(null)))
                },
                (Jagged::I64(jagged), Array::I64(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.into_iter()
                            .for_each(|cats| cats.push(null)))
                },
                (Jagged::Str(jagged), Array::Str(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.into_iter()
                            .for_each(|cats| cats.push(null.clone())))
                },
                (Jagged::Bool(jagged), Array::Bool(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.into_iter()
                            .for_each(|cats| cats.push(null)))
                },
                _ => return Err("categories and null must be homogeneously typed".into())
            }

            data_property.nature = Some(Nature::Categorical(NatureCategorical { categories }));

            return Ok(data_property.into());
        }

        // 1. check public arguments (constant n)
        let mut clamp_minimum = match public_arguments.get("min") {
            Some(min) => min.clone().array()?.clone().vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("min") {
                Some(min) => min.array()?.min_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .min_f64()?
            }
        };

        // 1. check public arguments (constant n)
        let mut clamp_maximum = match public_arguments.get("max") {
            Some(max) => max.array()?.clone().vec_f64(Some(num_columns))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("max") {
                Some(min) => min.array()?.max_f64()?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .max_f64()?
            }
        };

        if !clamp_minimum.iter().zip(clamp_maximum.clone()).all(|(min, max)| *min < max) {
            return Err("minimum is greater than maximum".into());
        }

        // the actual data bound (if it exists) may be tighter than the clamping parameters
        if let Ok(data_minimum) = data_property.min_f64_option() {
            clamp_minimum = clamp_minimum.iter().zip(data_minimum)
                // match on if the actual bound exists for each column, and remain conservative if not
                .map(|(clamp_min, optional_data_min)| match optional_data_min {
                    Some(data_min) => clamp_min.max(data_min), // tighter data bound is only applied here
                    None => clamp_min.clone()
                }).collect()
        }
        if let Ok(data_maximum) = data_property.max_f64_option() {
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
        let has_categorical = properties.contains_key("categories");

        if !has_categorical && !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !has_categorical && !properties.contains_key("max") {
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