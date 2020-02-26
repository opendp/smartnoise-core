use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints, get_constraint, get_literal};


use crate::proto;
use crate::hashmap;
use crate::components::{Component, Accuracy, Privatize, Expandable, Report};
use ndarray::Array;
use crate::utilities::serial::{Value, serialize_value, ArrayND};


impl Component for proto::DpMean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {
        Ok(get_constraint(constraints, "left")?.to_owned())

//        Ok(Constraint {
//            nullity: false,
//            releasable: true,
//            nature: Some(constraint_utils::Nature::Continuous(constraint_utils::NatureContinuous {
//                min: constraint_utils::get_min(&constraints, "data")?,
//                max: constraint_utils::get_max(&constraints, "data")?,
//            })),
//            num_records: constraint_utils::get_num_records(&constraints, "data")?,
//        })
    }

    fn is_valid(
        &self,
        _public_arguments: &HashMap<String, Value>,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<(), String> {
        let data_constraint = constraint_utils::get_constraint(&constraints, "data")?.clone();

        data_constraint.get_n()?;
        data_constraint.get_min_f64()?;
        data_constraint.get_max_f64()?;

        Ok(())
    }

    fn get_names(
        &self,
        _constraints: &NodeConstraints,
    ) -> Result<Vec<String>, String> {
        Err("get_names not implemented".to_string())
    }
}

impl Expandable for proto::DpMean {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        constraints: &constraint_utils::NodeConstraints,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>), String> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // mean
        current_id += 1;
        let id_mean = current_id.clone();
        graph_expansion.insert(id_mean, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            value: Some(proto::component::Value::Mean(proto::Mean {})),
            omit: true,
            batch: component.batch,
        });

        let sensitivity = Value::ArrayND(ArrayND::F64(Array::from(component.value.to_owned().unwrap()
                .compute_sensitivity(privacy_definition, constraints)
                .unwrap()).into_dyn()));

        // sensitivity literal
        current_id += 1;
        let id_sensitivity = current_id.clone();
        graph_expansion.insert(id_sensitivity, get_literal(&sensitivity, &component.batch));

        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_mean, "sensitivity".to_owned() => id_sensitivity],
            value: Some(proto::component::Value::LaplaceMechanism(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: true,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Privatize for proto::DpMean {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>> {
        let data_constraint = constraint_utils::get_constraint(constraints, "data").ok()?;
        let min = data_constraint.get_min_f64().ok()?;
        let max = data_constraint.get_max_f64().ok()?;
        let num_records = data_constraint.get_n().ok()?;

        Some(min
            .iter()
            .zip(max)
            .zip(num_records)
            .map(|((l, r), n)| (l - r) / n as f64)
            .collect())
    }
}

impl Accuracy for proto::DpMean {
    fn accuracy_to_privacy_usage(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _constraints: &constraint_utils::NodeConstraints,
        _accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _constraint: &constraint_utils::NodeConstraints,
    ) -> Option<f64> {
        None
    }
}

impl Report for proto::DpMean {
    fn summarize(
        &self,
        _constraints: &NodeConstraints,
    ) -> Option<String> {
        None
    }
}