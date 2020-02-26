use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, Nature, NatureContinuous, NodeConstraints, get_literal};

use crate::hashmap;
use crate::proto;

use crate::components::{Component, Expandable};

use crate::utilities::serial::{Vector1DNull, Value, serialize_value, ArrayND};
use itertools::Itertools;
use ndarray::Array;


impl Component for proto::Impute {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        let mut data_constraint = constraints.get("data").unwrap().clone();
        let mut min_constraint = constraints.get("min").unwrap().clone();
        let mut max_constraint = constraints.get("max").unwrap().clone();

        data_constraint.nullity = false;
        data_constraint.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::F64(data_constraint.get_min_f64_option()?.iter()
                .zip(min_constraint.get_min_f64_option()?)
                .zip(max_constraint.get_min_f64_option()?)
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
            max: Vector1DNull::F64(data_constraint.get_max_f64_option()?.iter()
                .zip(min_constraint.get_max_f64_option()?)
                .zip(max_constraint.get_max_f64_option()?)
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

        Ok(data_constraint)
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<(), String> {
        constraint_utils::get_constraint(constraints, "data")?;

        let has_min = constraints.contains_key("min") || constraints.get("data").unwrap().to_owned().get_min_f64().is_ok();
        let has_max = constraints.contains_key("max") || constraints.get("data").unwrap().to_owned().get_max_f64().is_ok();

        let has_continuous = has_min && has_max;
        let has_categorical = constraints.contains_key("categories");

        match has_continuous || has_categorical {
            true => Ok(()),
            false => Err("bounds are missing for the imputation component".to_string())
        }
    }

    fn get_names(
        &self,
        _constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}

impl Expandable for proto::Impute {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &constraint_utils::NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        let mut current_id = maximum_id;
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        let mut component = component.clone();

        if !constraints.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(constraints.get("data").unwrap().to_owned().get_min_f64()?).into_dyn()));
            graph_expansion.insert(id_min.clone(), get_literal(&value, &component.batch));
            component.arguments.insert("min".to_string(), id_min);
        }

        if !constraints.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(constraints.get("data").unwrap().to_owned().get_max_f64()?).into_dyn()));
            graph_expansion.insert(id_max, get_literal(&value, &component.batch));
            component.arguments.insert("max".to_string(), id_max);
        }

        graph_expansion.insert(component_id, component);
        Ok((current_id, graph_expansion))
    }
}