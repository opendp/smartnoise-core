use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Value, Jagged, Array};
use whitenoise_validator::utilities::{
    get_argument, array::broadcast_ndarray,
    privacy::{get_epsilon, get_delta, spread_privacy_usage}};
use crate::components::Evaluable;
use crate::utilities;
use whitenoise_validator::proto;
use ndarray;
use ndarray::{Axis, arr1};
use crate::utilities::mechanisms::exponential_mechanism;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        arguments: &NodeArguments
    ) -> Result<ReleaseNode> {

        let data = get_argument(arguments, "data")?.array()?;
        println!("data {:?}", data);
        let num_columns = data.num_columns()?;
        let mut data = match data {
            Array::F64(data) => data.clone(),
            Array::I64(data) => data.mapv(|v| v as f64),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.f64()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .map(|(mut data_column, (sensitivity, epsilon))|
                data_column.iter_mut().zip(sensitivity.iter())
                    .map(|(v, sens)| {
                        *v += utilities::mechanisms::laplace_mechanism(epsilon, *sens)?;
                        Ok(())
                    })
                    .collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::GaussianMechanism {
    fn evaluate(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        arguments: &NodeArguments
    ) -> Result<ReleaseNode> {

        let data = get_argument(arguments, "data")?.array()?;
        let num_columns = data.num_columns()?;
        let mut data = match data {
            Array::F64(data) => data.clone(),
            Array::I64(data) => data.mapv(|v| v as f64),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.f64()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;

        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter())
            .zip(epsilon.into_iter().zip(delta.into_iter()))
            .map(|((mut data_column, sensitivity), (epsilon, delta))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .map(|(v, sens)| {
                    *v += utilities::mechanisms::gaussian_mechanism(epsilon, delta, *sens)?;
                    Ok(())
                }).collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::SimpleGeometricMechanism {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;
        let num_columns = data.num_columns()?;
        let mut data = data.i64()?.to_owned();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.f64()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let lower = broadcast_ndarray(
            get_argument(arguments, "lower")?.array()?.i64()?, data.shape())?;

        let upper = broadcast_ndarray(
            get_argument(arguments, "upper")?.array()?.i64()?, data.shape())?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .zip(lower.gencolumns().into_iter().zip(upper.gencolumns().into_iter()))
            .map(|((mut data_column, (sensitivity, epsilon)), (lower, upper))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .zip(lower.iter().zip(upper.iter()))
                .map(|((v, sens), (c_min, c_max))| {
                    *v += utilities::mechanisms::simple_geometric_mechanism(
                        epsilon, *sens, *c_min, *c_max, privacy_definition.protect_elapsed_time)?;
                    Ok(())
                })
                .collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::ExponentialMechanism {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let candidates = get_argument(arguments, "candidates")?.jagged()?;

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.f64()?
            .iter().cloned().collect::<Vec<f64>>();

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let utilities = get_argument(arguments, "utilities")?.jagged()?.f64()?;

        let value = match candidates {
            Jagged::F64(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(*eps, *sens, cands, utils))
                    .collect::<Result<Vec<f64>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::I64(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(*eps, *sens, cands, utils))
                    .collect::<Result<Vec<i64>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::Str(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(*eps, *sens, cands, utils))
                    .collect::<Result<Vec<String>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::Bool(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(*eps, *sens, cands, utils))
                    .collect::<Result<Vec<bool>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            }
        };

        Ok(ReleaseNode {
            value,
            privacy_usages: Some(usages),
            public: true,
        })
    }
}
