use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};


use std::collections::HashMap;
use crate::base::{Vector2DJagged, Nature, Vector1DNull, NatureCategorical, NodeProperties, get_properties, ArrayND, get_literal};

use crate::{proto, base};

use crate::components::{Component, Expandable};

use crate::utilities::serial::{serialize_value};
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
        let mut data_property = properties.get("data").unwrap().clone();

        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(get_properties(properties, "data")?.get_min_f64_option()?.iter()
                .zip(get_properties(properties, "min")?.get_min_f64_option()?)
                .zip(get_properties(properties, "max")?.get_min_f64_option()?)
                .map(|((d, min), max)| vec![d, &min, &max]
                    .iter().filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .fold1(|l, r| l.min(r)))
                .collect()),
            max: Vector1DNull::F64(get_properties(properties, "data")?.get_max_f64_option()?.iter()
                .zip(get_properties(properties, "min")?.get_max_f64_option()?)
                .zip(get_properties(properties, "max")?.get_max_f64_option()?)
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<()> {

        if properties.contains_key("data") &&
            ((properties.contains_key("min") && properties.contains_key("max")) ||
                properties.contains_key("categories")) {
            return Ok(())
        }
        return Err("arguments missing to clamp component".into())
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