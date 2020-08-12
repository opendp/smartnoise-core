use crate::errors::*;

use crate::{proto, base, Warnable, Float};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::prepend;
use ndarray::prelude::*;
use std::convert::TryFrom;
use indexmap::map::IndexMap;

impl Component for proto::RawMoment {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type != DataType::Float {
            return Err("data: atomic type must be float".into())
        }

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_not_empty()?;

        let num_columns = data_property.num_columns()?;
        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::RawMoment(self.clone()),
            properties,
            lipschitz_constants: ndarray::Array::from_shape_vec(
                vec![1, num_columns as usize],
                (0..num_columns).map(|_| 1.).collect())?.into_dyn().into()
        });
        data_property.num_records = Some(1);
        data_property.dataset_id = Some(node_id as i64);
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
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                let k = i32::try_from(*k)?;
                let lower = data_property.lower_float()?;
                let upper = data_property.upper_float()?;
                let num_records = data_property.num_records()?;

                let row_sensitivity = lower.iter()
                    .zip(upper.iter())
                    .map(|(min, max)|
                        ((max - min).powi(self.order as i32) / (num_records as Float)).powi(k))
                    .collect::<Vec<Float>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            },
            _ => Err("RawMoment is only implemented for KNorm sensitivity spaces".into())
        }
    }
}