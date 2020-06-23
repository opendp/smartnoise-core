use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Value, Jagged, Array};
use whitenoise_validator::utilities::{
    get_argument, array::broadcast_ndarray,
    privacy::{get_epsilon, get_delta, spread_privacy_usage}};
use crate::components::Evaluable;
use crate::utilities;
use whitenoise_validator::{proto, Float, Integer};
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
        let num_columns = data.num_columns()?;
        let mut data = match data {
            Array::Float(data) => data.clone(),
            Array::Int(data) => data.mapv(|v| v as Float),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .try_for_each(|(mut data_column, (sensitivity, epsilon))|
                data_column.iter_mut().zip(sensitivity.iter())
                    .try_for_each(|(v, sens)| {
                        *v += utilities::mechanisms::laplace_mechanism(epsilon, *sens as f64)? as Float;
                        Ok::<_, Error>(())
                    }))?;

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
            Array::Float(data) => data.clone(),
            Array::Int(data) => data.mapv(|v| v as Float),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;

        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter())
            .zip(epsilon.into_iter().zip(delta.into_iter()))
            .try_for_each(|((mut data_column, sensitivity), (epsilon, delta))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .try_for_each(|(v, sens)| {
                    *v += utilities::mechanisms::gaussian_mechanism(epsilon, delta, *sens as f64)? as Float;
                    Ok::<_, Error>(())
                }))?;

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
        let mut data = data.int()?.to_owned();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| Error::from("privacy_definition must be known"))?;

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let lower = broadcast_ndarray(
            get_argument(arguments, "lower")?.array()?.int()?, data.shape())?;

        let upper = broadcast_ndarray(
            get_argument(arguments, "upper")?.array()?.int()?, data.shape())?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .zip(lower.gencolumns().into_iter().zip(upper.gencolumns().into_iter()))
            .try_for_each(|((mut data_column, (sensitivity, epsilon)), (lower, upper))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .zip(lower.iter().zip(upper.iter()))
                .try_for_each(|((v, sens), (c_min, c_max))| {
                    *v += utilities::mechanisms::simple_geometric_mechanism(
                        epsilon, *sens as f64,
                        *c_min as i64, *c_max as i64,
                        privacy_definition.protect_elapsed_time)? as Integer;
                    Ok::<_, Error>(())
                }))?;

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

        let sensitivity = get_argument(arguments, "sensitivity")?.array()?.float()?
            .iter().cloned().collect::<Vec<Float>>();

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let utilities = get_argument(arguments, "utilities")?.jagged()?.float()?;

        let value = match candidates {
            Jagged::Float(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(
                            *eps, *sens as f64, cands,
                            utils.into_iter().map(|v| v as f64).collect()))
                    .collect::<Result<Vec<Float>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::Int(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(
                            *eps, *sens as f64, cands,
                            utils.into_iter().map(|v| v as f64).collect()))
                    .collect::<Result<Vec<Integer>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::Str(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(
                            *eps, *sens as f64, cands,
                            utils.into_iter().map(|v| v as f64).collect()))
                    .collect::<Result<Vec<String>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Value::from(release_array)
            },
            Jagged::Bool(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(
                            *eps, *sens as f64, cands,
                            utils.into_iter().map(|v| v as f64).collect()))
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
