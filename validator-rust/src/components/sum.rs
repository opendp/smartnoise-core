use indexmap::map::IndexMap;
use ndarray::prelude::*;

use crate::{base, Float, proto, Warnable};
use crate::base::{AggregatorProperties, DataType, IndexKey, Nature, NatureContinuous, NodeProperties, SensitivitySpace, Value, ValueProperties, Vector1DNull};
use crate::components::{Component, Sensitivity};
use crate::errors::*;
use crate::utilities::prepend;

impl Component for proto::Sum {
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

        let num_columns = data_property.num_columns()?;
        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties::new(
            proto::component::Variant::Sum(self.clone()), properties, num_columns));

        if data_property.data_type != DataType::Float && data_property.data_type != DataType::Int {
            return Err("data: atomic type must be numeric".into())
        }
        data_property.nature = data_property.num_records.and_then(|n| Some(Nature::Continuous(NatureContinuous {
            lower: match data_property.data_type {
                DataType::Int => Vector1DNull::Int(data_property
                    .lower_int().ok()?.iter().map(|l| Some(l * n)).collect()),
                DataType::Float => Vector1DNull::Float(data_property
                    .lower_float().ok()?.iter().map(|l| Some(l * (n as Float))).collect()),
                _ => unreachable!()
            },
            upper: match data_property.data_type {
                DataType::Int => Vector1DNull::Int(data_property
                    .upper_int().ok()?.iter().map(|u| Some(u * n)).collect()),
                DataType::Float => Vector1DNull::Float(data_property
                    .upper_float().ok()?.iter().map(|u| Some(u * (n as Float))).collect()),
                _ => unreachable!()
            },
        })));
        data_property.num_records = Some(1);
        data_property.dataset_id = Some(node_id as i64);

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Sensitivity for proto::Sum {
    /// Sum sensitivities [are backed by the the proofs here](https://github.com/opendp/smartnoise-core/blob/master/whitepapers/sensitivities/sums/sums.pdf)
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {

        match sensitivity_type {

            SensitivitySpace::KNorm(k) => {

                let data_property = properties.get::<IndexKey>(&"data".into())
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_is_not_aggregated()?;
                data_property.assert_non_null()?;

                use proto::privacy_definition::Neighboring;
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                macro_rules! compute_sensitivity {
                    ($lower:expr, $upper:expr) => {
                        {
                            let row_sensitivity = match k {
                                1 | 2 => match neighboring_type {
                                    Neighboring::AddRemove => $lower.iter()
                                        .zip($upper.iter())
                                        .map(|(min, max)| min.abs().max(max.abs()))
                                        .collect::<Vec<_>>(),
                                    Neighboring::Substitute => $lower.iter()
                                        .zip($upper.iter())
                                        .map(|(min, max)| (max - min))
                                        .collect::<Vec<_>>()
                                }
                                _ => return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                            };

                            let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                            array_sensitivity.insert_axis_inplace(Axis(0));

                            Ok(array_sensitivity.into())
                        }
                    }
                }

                match data_property.data_type {
                    DataType::Int => compute_sensitivity!(data_property.lower_int()?, data_property.upper_int()?),
                    DataType::Float => compute_sensitivity!(data_property.lower_float()?, data_property.upper_float()?),
                    _ => return Err(Error::from("sum data must be numeric"))
                }
            }
            _ => Err("Sum sensitivity is only implemented for KNorm".into())
        }
    }
}