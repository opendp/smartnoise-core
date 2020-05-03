use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::prepend;
use ndarray::prelude::*;

impl Component for proto::KthRawSampleMoment {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::F64 {
            return Err("data: atomic type must be float".into())
        }

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_not_empty()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::KthRawSampleMoment(self.clone()),
            properties: properties.clone(),
            lipschitz_constant: (0..data_property.num_columns()?).map(|_| 1.).collect()
        });
        data_property.num_records = Some(1);
        Ok(data_property.into())
    }
}

impl Sensitivity for proto::KthRawSampleMoment {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                if k != &1 {
                    return Err("KthRawSampleMoment sensitivity is only implemented for KNorm of 1".into())
                }
                let lower = data_property.lower_f64()?;
                let upper = data_property.upper_f64()?;
                let num_records = data_property.num_records()?;

                let row_sensitivity = lower.iter().zip(upper.iter())
                    .map(|(min, max)| (max - min).powi(self.k as i32) / (num_records as f64))
                    .collect::<Vec<f64>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            },
            _ => Err("KthRawSampleMoment sensitivity is only implemented for KNorm of 1".into())
        }
    }
}