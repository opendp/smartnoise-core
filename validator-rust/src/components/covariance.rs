use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, ValueProperties};
use crate::utilities::prepend;

impl Component for proto::Covariance {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        if properties.contains_key("data") {
            let mut data_property = properties.get("data")
                .ok_or("data: missing")?.get_arraynd()
                .map_err(prepend("data:"))?.clone();

            // save a snapshot of the state when aggregating
            data_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone(),
            });

            let num_columns = data_property.get_num_columns()?;
            data_property.num_records = Some(1);
            data_property.num_columns = Some(num_columns * (num_columns + 1) / 2);

            // min/max of data is not known after computing covariance
            data_property.nature = None;
            return Ok(data_property.into());
        } else if properties.contains_key("left") && properties.contains_key("right") {
            let mut left_property = properties.get("left")
                .ok_or("left: missing")?.get_arraynd()
                .map_err(prepend("left:"))?.clone();

            let right_property = properties.get("right")
                .ok_or("right: missing")?.get_arraynd()
                .map_err(prepend("right:"))?.clone();

            // save a snapshot of the state when aggregating
            left_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::from(self.clone()),
                properties: properties.clone(),
            });

            let left_n = left_property.get_num_records()?;
            let right_n = right_property.get_num_records()?;

            if left_n != right_n {
                return Err("n for left and right must be equivalent".into());
            }

            left_property.nature = None;
            left_property.releasable = left_property.releasable && right_property.releasable;

            left_property.num_records = Some(1);
            left_property.num_columns = Some(left_property.get_num_columns()? * right_property.get_num_columns()?);

            return Ok(left_property.into());
        } else {
            return Err("either \"data\" for covariance, or \"left\" and \"right\" for cross-covariance must be supplied".into());
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::Covariance {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &Sensitivity,
    ) -> Result<Vec<f64>> {

        match sensitivity_type {
            Sensitivity::KNorm(k) => {

                let data_n;
                let differences = match (properties.get("data"), properties.get("left"), properties.get("right")) {
                    (Some(data_property), None, None) => {

                        // data: perform checks and prepare parameters
                        let data_property = data_property.get_arraynd()
                            .map_err(prepend("data:"))?.clone();
                        data_property.assert_is_not_aggregated()?;
                        data_property.assert_non_null()?;
                        let data_min = data_property.get_min_f64()?;
                        let data_max = data_property.get_max_f64()?;
                        data_n = data_property.get_num_records()? as f64;

                        // collect bound differences for upper triangle of matrix
                        data_min.iter().zip(data_max.iter()).enumerate()
                            .map(|(i, (left_min, left_max))| data_min.iter().zip(data_max.iter()).enumerate()
                                .filter(|(j, _)| i <= *j)
                                .map(|(_, (right_min, right_max))|
                                    (*left_max - *left_min) * (*right_max - *right_min))
                                .collect::<Vec<f64>>()).flat_map(|s| s).collect::<Vec<f64>>()
                    },
                    (None, Some(left_property), Some(right_property)) => {

                        // left side: perform checks and prepare parameters
                        let left_property = left_property.get_arraynd()
                            .map_err(prepend("left:"))?.clone();
                        left_property.assert_is_not_aggregated()?;
                        left_property.assert_non_null()?;
                        let left_n = left_property.get_num_records()?;
                        let left_min = left_property.get_min_f64()?;
                        let left_max = left_property.get_max_f64()?;

                        // right side: perform checks and prepare parameters
                        let right_property = right_property.get_arraynd()
                            .map_err(prepend("right:"))?.clone();
                        right_property.assert_is_not_aggregated()?;
                        right_property.assert_non_null()?;
                        let right_n = right_property.get_num_records()?;
                        let right_min = right_property.get_min_f64()?;
                        let right_max = right_property.get_max_f64()?;

                        // ensure conformability
                        if left_n != right_n {
                            return Err("n for left and right must be equivalent".into());
                        }
                        data_n = left_n as f64;

                        // collect bound differences for entire matrix
                        left_min.clone().iter().zip(left_max.clone())
                            .map(|(left_min, left_max)| right_min.iter().zip(right_max.iter())
                                .map(|(right_min, right_max)|
                                    (left_max - *left_min) * (right_max - *right_min))
                                .collect::<Vec<f64>>())
                            .flat_map(|s| s).collect::<Vec<f64>>()
                    }
                    _ => return Err("either \"data\" or \"left\" and \"right\" must be supplied".into())
                };

                let delta_degrees_of_freedom = if self.finite_sample_correction { 1 } else { 0 } as f64;
                let normalization = data_n - delta_degrees_of_freedom;

                use proto::privacy_definition::Neighboring;
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or::<Error>("neighboring definition must be either \"AddRemove\" or \"Substitute\"".into())?;

                let scaling_constant: f64 = match k {
                    1 | 2 => match neighboring_type {
                        Neighboring::AddRemove => data_n / (data_n + 1.) / normalization,
                        Neighboring::Substitute => 2. * (data_n - 1.) / data_n / normalization
                    },
                    _ => return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                };

                Ok(differences.iter()
                    .map(|difference| (difference * scaling_constant).powi(*k as i32))
                    .collect())
            },
            _ => Err("Covariance sensitivity is only implemented for KNorm".into())
        }

    }
}