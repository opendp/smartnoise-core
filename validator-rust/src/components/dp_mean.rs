use std::collections::HashMap;
use crate::utilities::constraint as constraint_utils;
use crate::utilities::constraint::{Constraint, NodeConstraints};

use crate::{base, components};
use crate::proto;
use crate::hashmap;
use crate::components::Component;
use crate::utilities::buffer::{serialize_proto_value, NodeEvaluation};
use ndarray::Array;

impl Component for proto::DpMean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint_utils::NodeConstraints,
    ) -> Result<Constraint, String> {

        Ok(Constraint {
            nullity: false,
            releasable: true,
            nature: Some(constraint_utils::Nature::Continuous(constraint_utils::NatureContinuous {
                min: constraint_utils::get_min(&constraints, "data")?,
                max: constraint_utils::get_max(&constraints, "data")?,
            })),
            num_records: constraint_utils::get_num_records(&constraints, "data")?,
        })
    }

    fn is_valid(
        &self,
        constraints: &constraint_utils::NodeConstraints,
    ) -> bool {
        // check these properties are Some
        if constraint_utils::get_min_f64(constraints, "data").is_err()
            || constraint_utils::get_min_f64(constraints, "data").is_err()
            || constraint_utils::get_num_records_u32(constraints, "data").is_err() {
            return false;
        }

        // all checks have passed
        true
    }

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

        let sensitivity = serialize_proto_value(
            &NodeEvaluation::F64(Array::from(component.value.to_owned().unwrap()
                .compute_sensitivity(privacy_definition, constraints)
                .unwrap()).into_dyn()))?;

        // sensitivity literal
        current_id += 1;
        let id_sensitivity = current_id.clone();
        graph_expansion.insert(id_sensitivity, proto::Component {
            arguments: hashmap![],
            value: Some(proto::component::Value::Literal(proto::Literal {
                value: Some(sensitivity),
                private: true
            })),
            omit: true,
            batch: component.batch
        });

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

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &NodeConstraints,
    ) -> Option<Vec<f64>> {
        let min = constraint_utils::get_min_f64(constraints, "data").unwrap();
        let max = constraint_utils::get_max_f64(constraints, "data").unwrap();
        let num_records = constraint_utils::get_num_records_u32(constraints, "data").unwrap() as f64;

        Some(min
            .iter().zip(max)
            .map(|(l, r)| (l - r) / num_records)
            .collect())
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint_utils::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &constraint_utils::NodeConstraints,
    ) -> Option<f64> {
        None
    }

    fn summarize(
        &self,
        constraints: &constraint_utils::NodeConstraints,
    ) -> String {
        "".to_string()
    }
}