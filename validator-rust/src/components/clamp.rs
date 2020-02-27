use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};


use std::collections::HashMap;
use crate::base::{Vector2DJagged, Nature, Vector1DNull, NatureCategorical, NodeProperties, ArrayND, get_literal};

use crate::{proto, base};

use crate::components::{Component, Expandable};

use crate::utilities::serial::serialize_value;
use itertools::Itertools;
use ndarray::Array;
use crate::base::{Value, Properties, NatureContinuous};


impl Component for proto::Clamp {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data").ok_or("data missing from Clamp")?.clone();
        let min_property = properties.get("min").ok_or("min missing from Clamp")?.clone();
        let max_property = properties.get("max").ok_or("max missing from Clamp")?.clone();

        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(data_property.get_min_f64_option()
                .or(min_property.get_min_f64_option())?.iter()
                .zip(min_property.get_min_f64_option()?)
                .zip(max_property.get_min_f64_option()?)
                .map(|((d, min), max)| vec![d, &min, &max]
                    .iter().filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .fold1(|l, r| l.min(r)))
                .collect()),
            max: Vector1DNull::F64(data_property.get_max_f64_option()
                .or(max_property.get_max_f64_option())?.iter()
                .zip(min_property.get_max_f64_option()?)
                .zip(max_property.get_max_f64_option()?)
                .map(|((d, min), max)| vec![d, &min, &max]
                    .iter().filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .fold1(|l, r| l.max(r)))
                .collect()),
        }));

        Ok(data_property)
    }

    fn is_valid(
        &self,
        properties: &base::NodeProperties,
    ) -> Result<()> {
        // ensure data is passed
        let data_props = properties.get("data")
            .ok_or::<Error>("data missing from Clamp".into())?;
        let min_props = properties.get("min");
        let max_props = properties.get("max");

        // min and max may either come from props, or as an argument
        let has_min = data_props.get_min_f64().is_ok()
            || (min_props.is_some() && min_props.unwrap().get_min_f64().is_ok());
        let has_max = data_props.get_max_f64().is_ok()
            || (max_props.is_some() && max_props.unwrap().get_min_f64().is_ok());

        if has_min && has_max {
            return Ok(())
        }

        // categories may either come from props, or as an argument
        let cat_props = properties.get("categories");
        let has_categories = data_props.get_categories().is_ok()
            || (cat_props.is_some() && cat_props.unwrap().get_categories().is_ok());

        if has_categories {
            return Ok(())
        }
        return Err("arguments missing to clamp component".into());
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::Clamp {
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
            println!("filling in min");
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

        graph_expansion.insert(component_id, component);
        Ok((current_id, graph_expansion))
    }
}