use crate::errors::*;

use std::collections::HashMap;

use crate::{proto};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull};
use crate::utilities::{prepend};
use ndarray::{arr1};


impl Component for proto::Count {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.num_records = Some(1);
        data_property.num_columns = Some(1);

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone()
        });

        let data_num_records = data_property.num_records;
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::I64(vec![data_num_records.or(Some(0))]),
            max: Vector1DNull::I64(vec![data_num_records]),
        }));
        data_property.data_type = DataType::I64;

        Ok(data_property.into())
    }
}

impl Aggregator for proto::Count {
    /// Count query sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/counts/counts.pdf).
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                // k has no effect on the sensitivity, and is ignored

                use proto::privacy_definition::Neighboring;
                use proto::privacy_definition::Neighboring::{Substitute, AddRemove};
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                let num_records = data_property.num_records;

                // SENSITIVITY DERIVATIONS
                let sensitivity: f64 = match (neighboring_type, num_records) {
                    // known N. Applies to any neighboring type.
                    (_, Some(_)) => 0.,

                    // unknown N. The sensitivity here is really zero-- artificially raised
                    (Substitute, None) => 1.,

                    // unknown N
                    (AddRemove, None) => 1.,
                };
                Ok(arr1(&[sensitivity]).into_dyn().into())
            },
            _ => Err("Count sensitivity is only implemented for KNorm".into())
        }
    }
}
