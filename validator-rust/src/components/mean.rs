use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, ValueProperties};
use crate::utilities::prepend;


impl Component for proto::Mean {
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

        Ok(data_property.into())
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

        match sensitivity_type {
            Sensitivity::KNorm(k) => {

                let data_property = properties.get("data")
                    .ok_or("data: missing")?.get_arraynd()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_non_null()?;
                data_property.assert_is_not_aggregated()?;
                let data_min = data_property.get_min_f64()?;
                let data_max = data_property.get_max_f64()?;
                let data_n = data_property.get_num_records()? as f64;

                // AddRemove vs. Substitute share the same bounds

                match k {
                    1 | 2 => Ok(data_min.iter()
                        .zip(data_max.iter())
                        .map(|(min, max)| ((max - min) / data_n).powi(*k as i32))
                        .collect()),
                    _ => Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                }
            },
            _ => Err("Mean sensitivity is only implemented for KNorm".into())
        }
    }
}