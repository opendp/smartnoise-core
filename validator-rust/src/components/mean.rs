use crate::errors::*;

use crate::{proto, base, Warnable, Float};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, IndexKey};
use crate::utilities::prepend;
use ndarray::prelude::*;
use indexmap::map::IndexMap;

impl Component for proto::Mean {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        data_property.assert_is_not_empty()?;

        let num_columns = data_property.num_columns()?;
        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties::new(
            proto::component::Variant::Mean(self.clone()), properties, num_columns));

        if data_property.data_type != DataType::Float {
            return Err("data: atomic type must be float".into())
        }

        data_property.num_records = Some(1);
        data_property.dataset_id = Some(node_id as i64);

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Sensitivity for proto::Mean {
    /// Mean sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/mean/mean.pdf).
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {
        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                let data_property = properties.get::<IndexKey>(&"data".into())
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_non_null()?;
                data_property.assert_is_not_aggregated()?;
                let data_lower = data_property.lower_float()?;
                let data_upper = data_property.upper_float()?;
                let data_n = data_property.num_records()? as Float;

                // AddRemove vs. Substitute share the same bounds

                let row_sensitivity = match k {
                    1 | 2 => data_lower.iter()
                        .zip(data_upper.iter())
                        .map(|(min, max)| (max - min) / data_n)
                        .collect::<Vec<Float>>(),
                    _ => return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                };

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            _ => Err("Mean sensitivity is only implemented for KNorm".into())
        }
    }
}