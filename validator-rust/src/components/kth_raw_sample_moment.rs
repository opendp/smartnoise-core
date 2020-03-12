use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, prepend, ValueProperties};


impl Component for proto::KthRawSampleMoment {
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

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::KthRawSampleMoment {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        match sensitivity_type {
            Sensitivity::KNorm(k) => {
                if k != &1 {
                    return Err("KthRawSampleMoment sensitivity is only implemented for KNorm of 1".into())
                }
                let min = data_property.get_min_f64()?;
                let max = data_property.get_max_f64()?;
                let num_records = data_property.get_num_records()?;

                Ok(min.iter()
                    .zip(max)
                    .map(|(min, max)| (max - min).powi(self.k as i32) / (num_records as f64))
                    .collect())
            },
            _ => return Err("KthRawSampleMoment sensitivity is only implemented for KNorm of 1".into())
        }
    }
}