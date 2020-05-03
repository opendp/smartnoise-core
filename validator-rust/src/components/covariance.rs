use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};
use crate::utilities::prepend;
use ndarray::prelude::*;

impl Component for proto::Covariance {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        if properties.contains_key("data") {
            let mut data_property = properties.get("data")
                .ok_or("data: missing")?.array()
                .map_err(prepend("data:"))?.clone();

            data_property.assert_is_not_empty()?;

            if !data_property.releasable {
                data_property.assert_is_not_aggregated()?;
            }

            let num_columns = data_property.num_columns()?;
            let num_columns = num_columns * (num_columns + 1) / 2;

            // save a snapshot of the state when aggregating
            data_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::Covariance(self.clone()),
                properties: properties.clone(),
                lipschitz_constant: (0..num_columns).map(|_| 1.).collect()
            });

            data_property.num_records = Some(1);
            data_property.num_columns = Some(num_columns);

            if data_property.data_type != DataType::F64 {
                return Err("data: atomic type must be float".into());
            }
            // min/max of data is not known after computing covariance
            data_property.nature = None;
            Ok(data_property.into())
        } else if properties.contains_key("left") && properties.contains_key("right") {
            let mut left_property = properties.get("left")
                .ok_or("left: missing")?.array()
                .map_err(prepend("left:"))?.clone();

            let right_property = properties.get("right")
                .ok_or("right: missing")?.array()
                .map_err(prepend("right:"))?.clone();


            if left_property.data_type != DataType::F64 {
                return Err("left: atomic type must be float".into());
            }
            if right_property.data_type != DataType::F64 {
                return Err("right: atomic type must be float".into());
            }
            left_property.assert_is_not_empty()?;
            right_property.assert_is_not_empty()?;

            if !left_property.releasable {
                left_property.assert_is_not_aggregated()?;
            }

            if !right_property.releasable {
                right_property.assert_is_not_aggregated()?;
            }

            let num_columns = left_property.num_columns()? * right_property.num_columns()?;

            // save a snapshot of the state when aggregating
            left_property.aggregator = Some(AggregatorProperties {
                component: proto::component::Variant::Covariance(self.clone()),
                properties: properties.clone(),
                lipschitz_constant: (0..num_columns).map(|_| 1.).collect()
            });

            left_property.nature = None;
            left_property.releasable = left_property.releasable && right_property.releasable;

            left_property.num_records = Some(1);
            left_property.num_columns = Some(num_columns);

            Ok(left_property.into())
        } else {
            Err("either \"data\" for covariance, or \"left\" and \"right\" for cross-covariance must be supplied".into())
        }
    }
}

impl Sensitivity for proto::Covariance {
    /// Covariance sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/covariance/covariance.pdf).
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {
        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                let data_n;
                let differences = match (properties.get("data"), properties.get("left"), properties.get("right")) {
                    (Some(data_property), None, None) => {

                        // data: perform checks and prepare parameters
                        let data_property = data_property.array()
                            .map_err(prepend("data:"))?.clone();
                        data_property.assert_is_not_aggregated()?;
                        data_property.assert_non_null()?;
                        let data_lower = data_property.lower_f64()?;
                        let data_upper = data_property.upper_f64()?;
                        data_n = data_property.num_records()? as f64;

                        // collect bound differences for upper triangle of matrix
                        data_lower.iter().zip(data_upper.iter()).enumerate()
                            .map(|(i, (left_min, left_max))| data_lower.iter().zip(data_upper.iter()).enumerate()
                                .filter(|(j, _)| i <= *j)
                                .map(|(_, (right_min, right_max))|
                                    (*left_max - *left_min) * (*right_max - *right_min))
                                .collect::<Vec<f64>>()).flatten().collect::<Vec<f64>>()
                    }
                    (None, Some(left_property), Some(right_property)) => {

                        // left side: perform checks and prepare parameters
                        let left_property = left_property.array()
                            .map_err(prepend("left:"))?.clone();
                        left_property.assert_is_not_aggregated()?;
                        left_property.assert_non_null()?;
                        let left_n = left_property.num_records()?;
                        let left_lower = left_property.lower_f64()?;
                        let left_upper = left_property.upper_f64()?;

                        // right side: perform checks and prepare parameters
                        let right_property = right_property.array()
                            .map_err(prepend("right:"))?.clone();
                        right_property.assert_is_not_aggregated()?;
                        right_property.assert_non_null()?;
                        let right_n = right_property.num_records()?;
                        let right_lower = right_property.lower_f64()?;
                        let right_upper = right_property.upper_f64()?;

                        // ensure conformability
                        if left_n != right_n {
                            return Err("n for left and right must be equivalent".into());
                        }
                        data_n = left_n as f64;

                        // collect bound differences for entire matrix
                        left_lower.iter().zip(left_upper.iter())
                            .map(|(left_min, left_max)| right_lower.iter().zip(right_upper.iter())
                                .map(|(right_min, right_max)|
                                    (left_max - *left_min) * (right_max - *right_min))
                                .collect::<Vec<f64>>())
                            .flatten().collect::<Vec<f64>>()
                    }
                    _ => return Err("either \"data\" or \"left\" and \"right\" must be supplied".into())
                };

                let delta_degrees_of_freedom = if self.finite_sample_correction { 1 } else { 0 } as f64;
                let normalization = data_n - delta_degrees_of_freedom;

                use proto::privacy_definition::Neighboring;
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                let scaling_constant: f64 = match k {
                    1 | 2 => match neighboring_type {
                        Neighboring::AddRemove => data_n / (data_n + 1.) / normalization,
                        Neighboring::Substitute => 2. * (data_n - 1.) / data_n / normalization
                    },
                    _ => return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                };

                let row_sensitivity = differences.iter()
                    .map(|difference| (difference * scaling_constant).powi(*k as i32))
                    .collect::<Vec<f64>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            _ => Err("Covariance sensitivity is only implemented for KNorm".into())
        }
    }
}