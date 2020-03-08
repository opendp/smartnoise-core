use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, Properties, NodeProperties, AggregatorProperties};

impl Component for proto::Covariance {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {

        if properties.contains_key("data") {
            let mut data_property = properties.get("data").unwrap().clone();
            data_property.assert_is_not_aggregated()?;

            // save a snapshot of the state when aggregating
            data_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone()
            });

            match data_property.num_columns {
                // number of rows is known if number of columns is known
                Some(num_columns) =>
                    data_property.num_records = (0..num_columns).map(|v| Some(v + 1)).collect(),

                // else number of rows is not known
                None =>
                    data_property.num_records = Vec::new()
            }

            // min/max of data is not known after computing covariance
            data_property.nature = None;
            return Ok(data_property);

        } else if properties.contains_key("left") && properties.contains_key("right") {
            let mut left_property = properties.get("left")
                .ok_or("left must be passed for cross-covariance")?.clone();
            left_property.assert_is_not_aggregated()?;

            let mut right_property = properties.get("right")
                .ok_or("right must be passed for cross-covariance")?.clone();
            right_property.assert_is_not_aggregated()?;

            // save a snapshot of the state when aggregating
            left_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone()
            });

            left_property.nature = None;
            left_property.releasable = left_property.releasable && right_property.releasable;

            return Ok(left_property);

        } else {
            return Err("either \"data\" for covariance, or \"left\" and \"right\" for cross-covariance must be supplied".into())
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::Covariance {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Result<Vec<f64>> {
        // TODO: cross-covariance
        let data_property = properties.get("data")
            .ok_or::<Error>("data must be passed to compute sensitivity".into())?;

        let min = data_property.get_min_f64()?;
        let max = data_property.get_max_f64()?;

        Ok(min.iter()
            .zip(max)
            .map(|(min, max)| (max - min) as f64)
            .collect())
    }
}