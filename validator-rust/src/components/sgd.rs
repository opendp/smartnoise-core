use std::collections::hash_map::RandomState;

use indexmap::map::IndexMap;

use crate::base::{DataType, IndexKey, NodeProperties, Value, ValueProperties};
use crate::components::Component;
use crate::errors::*;
use crate::utilities::prepend;
use crate::proto;
use crate::Warnable;

impl Component for proto::Dpsgd {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<IndexKey, &Value, RandomState>,
        properties: NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy_definition must be defined")?;

        if privacy_definition.protect_floating_point {
            return Err("Floating-point protections are enabled. The gaussian mechanism is susceptible to floating-point attacks.".into());
        }

        if privacy_definition.group_size == 0 {
            return Err("group size must be greater than zero".into());
        }

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::Float {
            return Err("data: atomic type must be float".into());
        }
        data_property.assert_is_not_aggregated()?;

        let theta_property = properties.get::<IndexKey>(&"theta".into())
            .ok_or("theta: missing")?.array()
            .map_err(prepend("theta:"))?.clone();

        if theta_property.data_type != DataType::Float {
            return Err("theta: atomic type must be float".into());
        }

        // println!("{:?}", theta_property);
        theta_property.assert_is_releasable()?;

        Ok(Warnable(theta_property.into(), vec![]))
    }
}