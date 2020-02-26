use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{hashmap, base};
use crate::proto;

use crate::components::{Component, Expandable};

use crate::utilities::serial::{serialize_value};
use itertools::Itertools;
use ndarray::Array;
use crate::base::{Properties, Vector1DNull, Nature, NatureContinuous, Value, NodeProperties, ArrayND, get_literal};


impl Component for proto::Impute {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data").unwrap().clone();
        let mut min_property = properties.get("min").unwrap().clone();
        let mut max_property = properties.get("max").unwrap().clone();

        data_property.nullity = false;
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(data_property.get_min_f64_option()?.iter()
                .zip(min_property.get_min_f64_option()?)
                .zip(max_property.get_min_f64_option()?)
                .map(|((d, min), max)| {
                    match d {
                        Some(_x) => vec![d, &min, &max]
                            .iter().filter(|x| x.is_some())
                            .map(|x| x.unwrap().clone())
                            .fold1(|l, r| l.min(r)),
                        // since there was no prior bound, nothing is known about the min
                        None => None
                    }
                })
                .collect()),
            max: Vector1DNull::F64(data_property.get_max_f64_option()?.iter()
                .zip(min_property.get_max_f64_option()?)
                .zip(max_property.get_max_f64_option()?)
                .map(|((d, min), max)| {
                    match d {
                        // if there was a prior bound
                        Some(_x) => vec![d, &min, &max]
                            .iter().filter(|x| x.is_some())
                            .map(|x| x.unwrap().clone())
                            .fold1(|l, r| l.max(r)),
                        // since there was no prior bound, nothing is known about the max
                        None => None
                    }
                })
                .collect()),
        }));

        Ok(data_property)
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<()> {
        base::get_properties(properties, "data")?;

        let has_min = properties.contains_key("min") || properties.get("data").unwrap().to_owned().get_min_f64().is_ok();
        let has_max = properties.contains_key("max") || properties.get("data").unwrap().to_owned().get_max_f64().is_ok();

        let has_continuous = has_min && has_max;
        let has_categorical = properties.contains_key("categories");

        match has_continuous || has_categorical {
            true => Ok(()),
            false => Err("bounds are missing for the imputation component".into())
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::Impute {
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