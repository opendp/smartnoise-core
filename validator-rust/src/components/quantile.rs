use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};

use crate::utilities::prepend;
use ndarray::prelude::*;


impl Component for proto::Quantile {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        data_property.assert_is_not_aggregated()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone(),
        });

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into())
        }

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
    }


}

impl Aggregator for proto::Quantile {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;
        data_property.assert_non_null()?;


        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                if k != &1 {
                    return Err("Quantile sensitivity is only implemented for KNorm of 1".into());
                }
                let min = data_property.min_f64()?;
                let max = data_property.max_f64()?;

                let row_sensitivity = min.iter().zip(max.iter())
                    .map(|(min, max)| (max - min))
                    .collect::<Vec<f64>>();

                Ok(Array::from(row_sensitivity).into_dyn().into())
            }
            SensitivitySpace::Exponential => {
                let num_columns = data_property.num_columns()?;
                let row_sensitivity = (0..num_columns).map(|_| 1.).collect::<Vec<f64>>();

                Ok(Array::from(row_sensitivity).into_dyn().into())
            },
            _ => Err("Quantile sensitivity is not implemented for the specified sensitivity type".into())
        }
    }
}