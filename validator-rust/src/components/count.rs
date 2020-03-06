use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, Properties, NodeProperties, AggregatorProperties, Vector2DJagged};

// TODO: more checks needed here

impl Component for proto::Count {
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
            component: proto::component::Variant::from(self.clone()),
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

impl Aggregator for proto::Count {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<Vec<f64>> {
        let data_property = properties.get("data")?;
        let num_columns = data_property.num_columns.clone().unwrap();

        // if n is set, and the number of categories is 2, then sensitivity is 1.
        // Otherwise, sensitivity is 2 (changing one person can alter two bins)
        Some(match data_property.get_n() {
            Ok(num_records) => match data_property.get_categories() {
                Ok(categories) => get_lengths(&categories).iter()
                    .map(|column_length| if column_length <= &2 {1.} else {2.})
                    .collect(),
                Err(_) => (0..num_columns).map(|_| 2.).collect()
            },
            Err(_) => (0..num_columns).map(|_| 2.).collect()
        })
    }
}

fn get_lengths(value: &Vector2DJagged) -> Vec<i64> {
    match value {
        Vector2DJagged::Bool(value) => value.iter()
            .map(|column| column.clone().unwrap().len() as i64).collect(),
        Vector2DJagged::F64(value) => value.iter()
            .map(|column| column.clone().unwrap().len() as i64).collect(),
        Vector2DJagged::I64(value) => value.iter()
            .map(|column| column.clone().unwrap().len() as i64).collect(),
        Vector2DJagged::Str(value) => value.iter()
            .map(|column| column.clone().unwrap().len() as i64).collect()
    }
}