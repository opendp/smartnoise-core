use crate::errors::*;

use std::collections::HashMap;

use crate::{proto};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull};
use ndarray::{arr1};


impl Component for proto::Count {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = match properties.get("data").ok_or("data: missing")?.clone() {
            ValueProperties::Array(data_property) => data_property,
            ValueProperties::Indexmap(data_property) => {
                data_property.assert_is_dataframe()?;
                data_property.properties.values().first()
                    .ok_or_else(|| Error::from("dataframe must have at least one column"))?.array()?.to_owned()
            },
            ValueProperties::Jagged(_) => return Err("Count is not implemented on jagged arrays".into()),
            ValueProperties::Function(_) => return Err("Count is not implemented for functions".into())
        };

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        data_property.num_records = Some(1);
        data_property.num_columns = Some(1);

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Count(self.clone()),
            properties: properties.clone(),
            lipschitz_constant: vec![1.]
        });

        let data_num_records = data_property.num_records;
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            lower: Vector1DNull::I64(vec![data_num_records.or(Some(0))]),
            upper: Vector1DNull::I64(vec![data_num_records]),
        }));
        data_property.data_type = DataType::I64;

        Ok(data_property.into())
    }
}

impl Sensitivity for proto::Count {
    /// Count query sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/counts/counts.pdf).
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {

        let num_records = match properties.get("data")
            .ok_or("data: missing")? {
            ValueProperties::Array(value) => {
                value.assert_is_not_aggregated()?;
                value.num_records
            },
            ValueProperties::Indexmap(value) => value.num_records,
            _ => return Err("data: must not be hashmap".into())
        };

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                // k has no effect on the sensitivity, and is ignored

                use proto::privacy_definition::Neighboring;
                use proto::privacy_definition::Neighboring::{Substitute, AddRemove};
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

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
