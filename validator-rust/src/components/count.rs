use crate::errors::*;


use crate::{proto, Warnable, base, Float, Integer};

use crate::components::{Component, Sensitivity};
use crate::base::{IndexKey, Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull};
use ndarray::{arr1};
use itertools::Itertools;
use indexmap::map::IndexMap;


impl Component for proto::Count {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let mut data_property = match properties.get::<IndexKey>(&"data".into()).ok_or("data: missing")?.clone() {
            ValueProperties::Array(data_property) => data_property,
            ValueProperties::Dataframe(data_property) => {
                data_property.children.get_index(0)
                    .ok_or_else(|| Error::from("dataframe must have at least one column"))?
                    .1.array()?.to_owned()
            },
            _ => return Err("Count is only implemented on arrays and dataframes".into())
        };

        if self.distinct && data_property.data_type == DataType::Float && data_property.nullity {
            return Err("distinct counts on floats require non-nullity".into())
        }

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        let c_stability = match properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")? {
            ValueProperties::Array(value) => {
                value.assert_is_not_aggregated()?;

                // overall c_stability is the maximum c_stability of any column
                vec![value.c_stability.iter().copied().fold1(|l, r| l.max(r))
                    .ok_or_else(|| "c_stability must be defined for each column")?]
            },
            ValueProperties::Dataframe(value) => {

                // overall c_stability is the maximal c_stability of any column
                vec![value.children.values()
                    .map(|v| v.array().map(|v| v.c_stability.clone()))
                    .collect::<Result<Vec<Vec<Float>>>>()?.into_iter()
                    .flatten()
                    .fold1(|l, r| l.max(r))
                    .ok_or_else(|| "c_stability must be defined for each column")?]
            },
            _ => return Err("data: must be an array or dataframe".into())
        };

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Count(self.clone()),
            properties,
            lipschitz_constants: ndarray::Array::from_shape_vec(vec![1, 1], vec![1.0])?.into_dyn().into()
        });
        data_property.c_stability = c_stability;

        let data_num_records = data_property.num_records.map(|v| v as Integer);
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            lower: Vector1DNull::Int(vec![data_num_records.or(Some(0))]),
            upper: Vector1DNull::Int(vec![data_num_records]),
        }));
        data_property.data_type = DataType::Int;
        data_property.dataset_id = Some(node_id as i64);
        data_property.num_records = Some(1);
        data_property.num_columns = Some(1);

        Ok(ValueProperties::Array(data_property).into())
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

        let (num_records, c_stability) = match properties.get(&IndexKey::from("data"))
            .ok_or("data: missing")? {
            ValueProperties::Array(value) => {
                value.assert_is_not_aggregated()?;

                // overall c_stability is the maximal c_stability of any column
                let c_stability = value.c_stability.iter().copied().fold1(|l, r| l.max(r))
                    .ok_or_else(|| "c_stability must be defined for each column")?;
                (value.num_records, c_stability)
            },
            ValueProperties::Dataframe(value) => {

                // overall c_stability is the maximal c_stability of any column
                let c_stability = value.children.values()
                    .map(|v| v.array().map(|v| v.c_stability.clone()))
                    .collect::<Result<Vec<Vec<Float>>>>()?.into_iter()
                    .flatten()
                    .fold1(|l, r| l.max(r))
                    .ok_or_else(|| "c_stability must be defined for each column")?;
                (value.num_records()?, c_stability)
            },
            _ => return Err("data: must be an array or dataframe".into())
        };

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                // k has no effect on the sensitivity, and is ignored

                use proto::privacy_definition::Neighboring::{self, Substitute, AddRemove};

                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                // SENSITIVITY DERIVATIONS
                let sensitivity: Float = match (neighboring_type, num_records) {
                    // known N. Applies to any neighboring type.
                    (_, Some(_)) => 0.,

                    // unknown N. The sensitivity here is really zero-- artificially raised
                    (Substitute, None) => 1.,

                    // unknown N
                    (AddRemove, None) => 1.,
                };
                Ok((arr1(&[sensitivity]).into_dyn() * c_stability).into())
            },
            _ => Err("Count sensitivity is only implemented for KNorm".into())
        }
    }
}
