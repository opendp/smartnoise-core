use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, prepend, ValueProperties};

impl Component for proto::Sum {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone(),
        });

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::Sum {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &Sensitivity,
    ) -> Result<Vec<f64>> {

        match sensitivity_type {

            Sensitivity::KNorm(k) => {

                let data_property = properties.get("data")
                    .ok_or("data: missing")?.get_arraynd()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_is_not_aggregated()?;
                data_property.assert_non_null()?;
                let data_min = data_property.get_min_f64()?;
                let data_max = data_property.get_max_f64()?;

                use proto::privacy_definition::Neighboring;
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or::<Error>("neighboring definition must be either \"AddRemove\" or \"Substitute\"".into())?;

                match k {
                    1 => Ok(match neighboring_type {
                        Neighboring::AddRemove => data_min.iter().zip(data_max.iter())
                            .map(|(min, max)| min.abs().max(max.abs()))
                            .collect::<Vec<f64>>(),
                        Neighboring::Substitute => data_min.iter().zip(data_max.iter())
                            .map(|(min, max)| max - min)
                            .collect::<Vec<f64>>()
                    }),
                    2 => Ok(match neighboring_type {
                        Neighboring::AddRemove => data_min.iter().zip(data_max.iter())
                            .map(|(min, max)| min.powi(2).max(max.powi(2)))
                            .collect::<Vec<f64>>(),
                        Neighboring::Substitute => data_min.iter().zip(data_max.iter())
                            .map(|(min, max)| (max - min).powi(2))
                            .collect::<Vec<f64>>()
                    }),
                    _ => Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                }
            }
            _ => Err("Sum sensitivity is only implemented for KNorm of 1".into())
        }
    }
}