use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, Properties, NodeProperties, AggregatorProperties};

// TODO: more checks needed here

impl Component for proto::Mean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data must be passed to Mean")?.clone();

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(proto::Mean {}),
            properties: properties.clone()
        });

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::Mean {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<Vec<f64>> {
        let data_property = properties.get("data")?;

        let min = data_property.get_min_f64().ok()?;
        let max = data_property.get_max_f64().ok()?;
        let num_records = data_property.get_n().ok()?;

        Some(min
            .iter()
            .zip(max)
            .zip(num_records)
            .map(|((min, max), n)| (max - min) / n as f64)
            .collect())
    }
}