use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base, Warnable};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::prepend;
use ndarray::prelude::*;
use std::convert::TryFrom;

impl Component for proto::RawMoment {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
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
            component: proto::component::Variant::RawMoment(self.clone()),
            properties: properties.clone(),
            lipschitz_constant: (0..data_property.num_columns()?).map(|_| 1.).collect()
        });
        data_property.num_records = Some(1);
        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Sensitivity for proto::RawMoment {
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
                let k = i32::try_from(*k)?;
                let lower = data_property.lower_f64()?;
                let upper = data_property.upper_f64()?;
                let num_records = data_property.num_records()?;
                let c_stability = data_property.c_stability;

                let row_sensitivity = lower.iter()
                    .zip(upper.iter())
                    .zip(c_stability.iter())
                    .map(|((min, max), c_stab)|
                        (((max - min) * c_stab).powi(self.order as i32) / (num_records as f64)).powi(k))
                    .collect::<Vec<f64>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            },
            _ => Err("RawMoment is only implemented for KNorm sensitivity spaces".into())
        }
    }
}