use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Vector2DJagged, standardize_categorical_argument, Sensitivity, ValueProperties, prepend};

impl Component for proto::Count {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone()
        });

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
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
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;

        let num_columns = data_property.get_num_columns()?;

        match sensitivity_type {

            Sensitivity::KNorm(k) => {
                if k != &1 {
                    return Err("Count sensitivity is only implemented for KNorm of 1".into())
                }
                // if n is set, and the number of categories is 2, then sensitivity is 1.
                // Otherwise, sensitivity is 2 (changing one person can alter two bins)
                Ok(match data_property.get_num_records() {
                    // known n
                    Ok(_num_records) => match data_property.get_categories() {
                        // by known categories
                        Ok(categories) => get_lengths(&categories).iter()
                            .map(|column_length| if column_length <= &2 {1.} else {2.})
                            .collect(),

                        // categories not set (useless: noisy estimate of number of rows, when number of rows is known)
                        Err(_) => (0..num_columns).map(|_| 1.).collect()
                    },
                    // unknown n
                    Err(_) => match data_property.get_categories() {
                        // by known categories
                        Ok(_categories) => (0..num_columns).map(|_| 2.).collect(),
                        // categories not set (estimate of number of rows)
                        Err(_) => (0..num_columns).map(|_| 1.).collect(),
                    }
                })
            },
            _ => return Err("Count sensitivity is only implemented for KNorm of 1".into())
        }
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