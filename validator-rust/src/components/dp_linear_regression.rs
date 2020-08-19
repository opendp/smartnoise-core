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
        let mut arguments = indexmap!["data_x".into() => id_data_x, "data_y".into() => id_data_y];

        match self.implementation.to_lowercase().as_str() {
            "theil-sen" => (),
            "theil-sen-k-match" => {
                arguments.insert("k".into(), component.arguments().get::<base::IndexKey>(&"k".into()).copied().unwrap_or_else(|| {
                    maximum_id += 1;
                    let id_k = maximum_id.to_owned();
                    let value = Value::from(100);
                    expansion.properties.insert(id_k, infer_property(&value, None)?);
                    let (patch_node, release) = get_literal(value, component.submission)?;
                    expansion.computation_graph.insert(id_k, patch_node);
                    expansion.releases.insert(id_k, release);
                    maximum_id
                }));
            }
            _ => return Err(Error::from("Invalid implementation argument"))
        }

        maximum_id += 1;
        let id_lin_reg = maximum_id;

        expansion.computation_graph.insert(id_lin_reg, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(
                arguments)),
            variant: Some(proto::component::Variant::TheilSen {
                privacy_usage: self.privacy_usage.clone()
            }),
            omit: true,
            submission: component.submission,
        });

        expansion.traversal.push(id_lin_reg);

        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_lin_reg])),
            variant: Some(proto::component::Variant::DpMedian(proto::DpMedian {
                mechanism: self.median_implementation.clone(),
                privacy_usage: self.privacy_usage.clone(),
                interpolation: "midpoint".to_string(),
            })),
            omit: component.omit,
            submission: component.submission,
        });

        Ok(expansion)
    }
}

