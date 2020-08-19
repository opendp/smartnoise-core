use crate::errors::*;


use crate::{proto, base, Integer};
use crate::components::{Expandable, Report};
use ndarray::arr0;

use crate::base::{IndexKey, NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, privacy_usage_to_json, AlgorithmInfo, value_to_json};
use crate::utilities::get_literal;
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;


impl Expandable for proto::DpLinearRegression {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();
        let id_x = *component.arguments().get::<base::IndexKey>(&"data_x".into())
            .ok_or_else(|| Error::from("data must be provided as an argument"))?;
        let id_y = *component.arguments().get::<base::IndexKey>(&"data_y".into())
            .ok_or_else(|| Error::from("data must be provided as an argument"))?;

        // Question: Why do we increment this?
        maximum_id += 1;
        let id_lin_reg = maximum_id;

        if self.implementation.to_lowercase().as_str() == "theil-sen" {
            expansion.computation_graph.insert(id_lin_reg, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["data_x".into() => id_data_x, "data_y".into() => id_data_y])),
                variant: Some(proto::component::Variant::TheilSen {
                    privacy_usage: self.privacy_usage.clone()
                }),
                omit: true,
                submission: component.submission,
            });
        } else if self.mechanism.to_lowercase().as_str() == "theil-sen-k-match" {
            let k = *component.arguments().get::<base::IndexKey>(&"k".into())
                .ok_or_else(|| Error::from("k must be provided as an argument to k-match"))?;
            expansion.computation_graph.insert(id_lin_reg, proto::Component {
                arguments: Some(proto::ArgumentNodeIds::new(
                    indexmap!["data_x".into() => id_data_x, "data_y".into() => id_data_y, "k".into() => id_k])),
                variant: Some(proto::component::Variant::TheilSenKMatch {
                    privacy_usage: self.privacy_usage.clone()
                }),
                omit: true,
                submission: component.submission,
            });
        } else {
            Error("Invalid implementation argument")
        }
    Ok(expansion)
    }
}

