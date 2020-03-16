use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivityType, prepend, ValueProperties, Sensitivity};

impl Component for proto::Covariance {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {

        if properties.contains_key("data") {
            let mut data_property = properties.get("data")
                .ok_or("data: missing")?.get_arraynd()
                .map_err(prepend("data:"))?.clone();

            data_property.assert_is_not_aggregated()?;

            // save a snapshot of the state when aggregating
            data_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone()
            });

            data_property.num_records = data_property.num_columns.clone();

            // min/max of data is not known after computing covariance
            data_property.nature = None;
            return Ok(data_property.into());

        } else if properties.contains_key("left") && properties.contains_key("right") {
            let mut left_property = properties.get("left")
                .ok_or("left: missing")?.get_arraynd()
                .map_err(prepend("left:"))?.clone();

            let right_property = properties.get("right")
                .ok_or("right: missing")?.get_arraynd()
                .map_err(prepend("right:"))?.clone();

            left_property.assert_is_not_aggregated()?;
            right_property.assert_is_not_aggregated()?;

            // save a snapshot of the state when aggregating
            left_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone()
            });

            left_property.nature = None;
            left_property.releasable = left_property.releasable && right_property.releasable;

            return Ok(left_property.into());

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
        sensitivity_type: &SensitivityType
    ) -> Result<Sensitivity> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();

        let right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        match sensitivity_type {
            SensitivityType::KNorm(k) => {
                if k != &1 {
                    return Err("Covariance sensitivity is only implemented for KNorm of 1".into())
                }

                // check that all properties are satisfied
//                println!("covariance left");
                let left_n = left_property.get_num_records()?;
                left_property.get_min_f64()?;
                left_property.get_max_f64()?;
                left_property.assert_non_null()?;

                // check that all properties are satisfied
//                println!("covariance right");
                let right_n = right_property.get_num_records()?;
                right_property.get_min_f64()?;
                right_property.get_max_f64()?;
                right_property.assert_non_null()?;

                if left_n != right_n {
                    return Err("n for left and right must be equivalent".into());
                }

                // TODO: derive proper propagation of covariance property
                left_property.num_records = Some(1);
                left_property.releasable = true;

                // TODO: cross-covariance
                let mut data_property = properties.get("data")
                    .ok_or("data: missing")?.get_arraynd()
                    .map_err(prepend("data:"))?.clone();

                let min = data_property.get_min_f64()?;
                let max = data_property.get_max_f64()?;

                Ok(vec![min.iter()
                    .zip(max)
                    .map(|(min, max)| (max - min) as f64)
                    .collect()])
            },
            _ => return Err("Covariance sensitivity is only implemented for KNorm of 1".into())
        }
    }
}