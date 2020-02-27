use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{hashmap, base};
use crate::proto;

use crate::components::{Component, Expandable};
use ndarray::Array;
use crate::utilities::serial::{serialize_value};
use crate::base::{Value, Properties, ArrayND, NodeProperties, get_literal};


impl Component for proto::Resize {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data").unwrap().clone();

        println!("resize metadata:");
//        println!("min: {:?}", min_property);
//        println!("max: {:?}", max_property);
        println!("properties: {:?}", properties);
        println!("public arguments: {:?}", public_arguments);
        // when resizing, nullity may become true to add additional rows
        data_property.nullity = true;
        data_property.num_records = match public_arguments.get("n").unwrap() {
            Value::ArrayND(array) => match array {
                ArrayND::I64(array) => match array.ndim() {
                    0 => (0..data_property.num_columns.unwrap())
                        .collect::<Vec<i64>>().iter().map(|_x| Some(array.first().unwrap().clone())).collect(),
                    _ => return Err("n must be a scalar".into())
                }
                _ => return Err("n must be an integer".into())
            }
            _ => return Err("n must be packed inside an ArrayND".into())
        };

        Ok(data_property)
    }

    fn is_valid(
        &self,
        properties: &base::NodeProperties,
    ) -> Result<()> {
        // TODO: stricter checks for bounds
        properties.get("n")
            .ok_or::<Error>("n is missing from resize".into())?;

        Ok(())
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
        privacy_definition: &proto::PrivacyDefinition,
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