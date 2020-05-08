use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, JaggedProperties};

use crate::utilities::prepend;
use ndarray::prelude::*;


impl Component for proto::Quantile {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_not_empty()?;

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into());
        }

        Ok(match public_arguments.get("candidates") {
            Some(candidates) => {
                let candidates = candidates.jagged()?;

                if data_property.data_type != candidates.data_type() {
                    return Err("data_type of data must match data_type of candidates".into())
                }

                JaggedProperties {
                    num_records: Some(candidates.num_records()),
                    nullity: false,
                    aggregator: Some(AggregatorProperties {
                        component: proto::component::Variant::Quantile(self.clone()),
                        properties: properties.clone(),
                        lipschitz_constant: (0..data_property.num_columns()?).map(|_| 1.).collect()
                    }),
                    nature: None,
                    data_type: DataType::F64,
                    releasable: false
                }.into()
            },
            None => {
                // save a snapshot of the state when aggregating
                data_property.aggregator = Some(AggregatorProperties {
                    component: proto::component::Variant::Quantile(self.clone()),
                    properties: properties.clone(),
                    lipschitz_constant: (0..data_property.num_columns()?).map(|_| 1.).collect()
                });

                data_property.num_records = Some(1);
                data_property.nature = None;

                data_property.into()
            }
        })
    }
}

impl Sensitivity for proto::Quantile {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
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
                let lower = data_property.lower_f64()?;
                let upper = data_property.upper_f64()?;

                let row_sensitivity = lower.iter().zip(upper.iter())
                    .map(|(min, max)| (max - min))
                    .collect::<Vec<f64>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            SensitivitySpace::Exponential => {
                let num_columns = data_property.num_columns()?;

                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;
                use proto::privacy_definition::Neighboring;
                let cell_sensitivity = match neighboring_type {
                    Neighboring::AddRemove => self.alpha.max(1. - self.alpha),
                    Neighboring::Substitute => 1.
                };
                let row_sensitivity = (0..num_columns).map(|_| cell_sensitivity).collect::<Vec<f64>>();
                let array_sensitivity = Array::from(row_sensitivity).into_dyn();
                // array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            _ => Err("Quantile sensitivity is not implemented for the specified sensitivity space".into())
        }
    }
}
