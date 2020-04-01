use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties};
use crate::utilities::prepend;
use ndarray::prelude::*;

impl Component for proto::Variance {
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
            properties: properties.clone()
        });

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
    }


}

impl Aggregator for proto::Variance {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {

        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {

                let data_property = properties.get("data")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_non_null()?;
                data_property.assert_is_not_aggregated()?;
                let data_min = data_property.min_f64()?;
                let data_max = data_property.max_f64()?;
                let data_n = data_property.num_records()? as f64;

                let delta_degrees_of_freedom = if self.finite_sample_correction { 1 } else { 0 } as f64;
                let normalization = data_n - delta_degrees_of_freedom;

                use proto::privacy_definition::Neighboring;
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or::<Error>("neighboring definition must be either \"AddRemove\" or \"Substitute\"".into())?;

                let scaling_constant: f64 = match k {
                    1 | 2 => match neighboring_type {
                        Neighboring::AddRemove => data_n / (data_n + 1.) / normalization,
                        Neighboring::Substitute => (data_n - 1.) / data_n / normalization
                    },
                    _ => return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                };

                let row_sensitivity = data_min.iter().zip(data_max.iter())
                    .map(|(min, max)| ((max - min).powi(2) * scaling_constant).powi(*k as i32))
                    .collect::<Vec<f64>>();

                Ok(Array::from(row_sensitivity).into_dyn().into())
            },
            _ => Err("Variance sensitivity is only implemented for KNorm of 1".into())
        }
    }
}