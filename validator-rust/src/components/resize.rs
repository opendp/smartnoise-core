use std::collections::HashMap;
use crate::utilities::properties as property_utils;
use crate::utilities::properties::{Properties, NodeProperties, get_literal};

use crate::hashmap;
use crate::proto;

use crate::components::{Component, Expandable};
use ndarray::Array;
use crate::utilities::serial::{Value, ArrayND, serialize_value};



impl Component for proto::Resize {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &property_utils::NodeProperties,
    ) -> Result<Properties, String> {
        let mut data_property = properties.get("data").unwrap().clone();

        // when resizing, nullity may become true to add additional rows
        data_property.nullity = true;
        data_property.num_records = match public_arguments.get("n").unwrap() {
            Value::ArrayND(array) => match array {
                ArrayND::I64(array) => match array.ndim() {
                    0 => (0..data_property.num_columns.unwrap())
                        .collect::<Vec<i64>>().iter().map(|_x| Some(array.first().unwrap().clone())).collect(),
                    _ => return Err("n must be a scalar".to_string())
                }
                _ => return Err("n must be an integer".to_string())
            }
            _ => return Err("n must be packed inside an ArrayND".to_string())
        };

        Ok(data_property)
    }

    fn is_valid(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &property_utils::NodeProperties,
    ) -> Result<(), String> {
        // TODO: stricter checks for bounds
        property_utils::get_properties(properties, "n")?;

        Ok(())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}

impl Expandable for proto::Resize {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &property_utils::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        let mut current_id = maximum_id;
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_min_f64()?).into_dyn()));
            graph_expansion.insert(id_min.clone(), get_literal(&value, &component.batch));
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_max_f64()?).into_dyn()));
            graph_expansion.insert(id_max, get_literal(&value, &component.batch));
            component.arguments.insert("max".to_string(), id_max);
        }

        if !properties.contains_key("n") {
            current_id += 1;
            let id_n = current_id.clone();
            let value = Value::ArrayND(ArrayND::I64(Array::from_shape_vec(
                (), properties.get("data").unwrap().to_owned().get_n()?)
                .unwrap().into_dyn()));

            graph_expansion.insert(id_n, get_literal(&value, &component.batch));
            component.arguments.insert("n".to_string(), id_n);
        }

        graph_expansion.insert(component_id, component);
        Ok((current_id, graph_expansion))
    }
}