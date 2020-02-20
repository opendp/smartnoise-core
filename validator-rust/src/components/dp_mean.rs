use std::collections::HashMap;
use crate::utilities::constraint;
use crate::utilities::constraint::Constraint;

use crate::base;
use crate::proto;
use crate::hashmap;
use crate::components::Component;

impl Component for proto::DpMean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_constraint(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> Constraint {
        Constraint {
            nullity: vec![false],
            releasable: false,
            nature: None,
            num_records: None
        }
    }

    fn is_valid(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> bool {
        false
    }

    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        maximum_id: u32,
        component_id: u32,
        constraints: &constraint::NodeConstraints,
    ) -> (u32, HashMap<u32, proto::Component>) {
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
        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_mean],
            value: Some(proto::component::Value::LaplaceMechanism(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone(),
                sensitivity: component.value.to_owned().unwrap()
                    .compute_sensitivity(
                        privacy_definition,
                        constraints.get("data").unwrap())
                    .unwrap(),
            })),
            omit: true,
            batch: component.batch,
        });

        (current_id, graph_expansion)
    }

    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &Constraint,
    ) -> Option<f64> {
        None
    }

    fn accuracy_to_privacy_usage(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraints: &constraint::NodeConstraints,
        accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        constraint: &constraint::NodeConstraints,
    ) -> Option<f64> {
        None
    }

    fn summarize(
        &self,
        constraints: &constraint::NodeConstraints,
    ) -> String {
        "".to_string()
    }
}