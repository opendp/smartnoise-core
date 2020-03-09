use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, Properties, NodeProperties, AggregatorProperties, Sensitivity};

// TODO: more checks needed here

impl Component for proto::Mean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data must be passed to Mean")?.clone();

        data_property.assert_is_not_aggregated()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone()
        });

        data_property.num_records = data_property.get_categories_lengths()?;

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
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>> {
        let data_property = properties.get("data")
            .ok_or::<Error>("data must be passed to compute sensitivity".into())?;

        data_property.assert_is_not_aggregated()?;
        data_property.assert_non_null()?;

        match sensitivity_type {
            Sensitivity::KNorm(k) => {
                if k != &1 {
                    return Err("Mean sensitivity is only implemented for KNorm of 1".into())
                }
                let min = data_property.get_min_f64()?;
                let max = data_property.get_max_f64()?;
                let num_records = data_property.get_n()?;

                Ok(min
                    .iter()
                    .zip(max)
                    .zip(num_records)
                    .map(|((min, max), n)| (max - min) / n as f64)
                    .collect())
            },
            _ => return Err("Mean sensitivity is only implemented for KNorm of 1".into())
        }
    }
}