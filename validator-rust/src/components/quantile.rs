use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;

use crate::components::{Component, Sensitivity, Utility};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};

use crate::utilities::prepend;
use ndarray::prelude::*;
use crate::utilities::serial::serialize_release;


impl Component for proto::Quantile {
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

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_not_empty()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Quantile(self.clone()),
            properties: properties.clone(),
        });

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into());
        }

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
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
            _ => Err("Quantile sensitivity is not implemented for the specified sensitivity type".into())
        }
    }
}

impl Utility for proto::Quantile {
    fn get_utility(
        &self,
        properties: &NodeProperties,
    ) -> Result<proto::Function> {
        use crate::bindings;

        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        let num_records = data_property.num_records()?;

        let mut analysis = bindings::Analysis::new();

        let data = analysis.literal().enter();
        let candidate = analysis.literal().enter();

        // compute #(Z < x)
        let count_z_lt_x = {
            let mask = analysis.less_than(data, candidate).enter();
            let filtered = analysis.filter(data, mask).enter();
            analysis.count(filtered).enter()
        };

        // compute weighted difference
        let abs_diff = {
            let n = analysis.literal().value(num_records.into()).enter();
            let alpha = analysis.literal().value(self.alpha.into()).enter();
            let alpha_inv = analysis.literal().value((1. - self.alpha).into()).enter();
            let count_z_gt_x = analysis.subtract(n, count_z_lt_x).enter();

            let left = analysis.multiply(alpha_inv, count_z_lt_x).enter();
            let right = analysis.multiply(alpha, count_z_gt_x).enter();
            let diff = analysis.subtract(left, right).enter();
            analysis.abs(diff).enter()
        };

        let utility = {
            let optimal = analysis.literal()
                .value((self.alpha.max(1. - self.alpha) * num_records as f64).into()).enter();
            analysis.subtract(optimal, abs_diff).enter()
        };

        Ok(proto::Function {
            computation_graph: Some(proto::ComputationGraph { value: analysis.components }),
            release: Some(serialize_release(analysis.release)),
            arguments: hashmap!["candidate".to_string() => candidate, "dataset".to_string() => data],
            outputs: hashmap!["utility".to_string() => utility]
        })
    }
}