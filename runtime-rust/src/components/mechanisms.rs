use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{ReleaseNode, Value, Jagged, Array};
use whitenoise_validator::utilities::{
    take_argument, array::broadcast_ndarray,
    privacy::{get_epsilon, get_delta, spread_privacy_usage}};
use crate::components::Evaluable;
use crate::utilities;
use whitenoise_validator::{proto, Float, Integer};
use ndarray::{Axis, arr1};
use crate::utilities::mechanisms::exponential_mechanism;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        mut arguments: NodeArguments
    ) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let data = take_argument(&mut arguments, "data")?.array()?;
        let num_columns = data.num_columns()?;
        let mut data = match data {
            Array::Float(data) => data,
            Array::Int(data) => data.mapv(|v| v as Float),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .try_for_each(|(mut data_column, (sensitivity, epsilon))|
                data_column.iter_mut().zip(sensitivity.iter())
                    .try_for_each(|(v, sens)| {
                        *v += utilities::mechanisms::laplace_mechanism(
                            epsilon, *sens as f64, enforce_constant_time)? as Float;
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
        privacy_definition: &Option<proto::PrivacyDefinition>,
        mut arguments: NodeArguments
    ) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let data = take_argument(&mut arguments, "data")?.array()?;
        let num_columns = data.num_columns()?;
        let mut data = match data {
            Array::Float(data) => data,
            Array::Int(data) => data.mapv(|v| v as Float),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;

        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter())
            .zip(epsilon.into_iter().zip(delta.into_iter()))
            .try_for_each(|((mut data_column, sensitivity), (epsilon, delta))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .try_for_each(|(v, sens)| {
                    *v += utilities::mechanisms::gaussian_mechanism(
                        epsilon, delta, *sens as f64, self.analytic, enforce_constant_time)? as Float;
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
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let data = take_argument(&mut arguments, "data")?.array()?;
        let num_columns = data.num_columns()?;
        let mut data = data.int()?.to_owned();

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.float()?;

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let lower = broadcast_ndarray(
            take_argument(&mut arguments, "lower")?.array()?.int()?, data.shape())?;

        let upper = broadcast_ndarray(
            take_argument(&mut arguments, "upper")?.array()?.int()?, data.shape())?;

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
                        enforce_constant_time)? as Integer;
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
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let candidates = take_argument(&mut arguments, "candidates")?.jagged()?;

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.float()?
            .iter().cloned().collect::<Vec<Float>>();

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let utilities = take_argument(&mut arguments, "utilities")?.jagged()?.float()?;

        let value = match candidates {
            Jagged::Float(candidates) => {
                let release_vec = candidates.iter().zip(utilities)
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|((cands, utils), (sens, eps))|
                        exponential_mechanism(
                            *eps, *sens as f64, cands,
                            utils.into_iter().map(|v| v as f64).collect(),
                            enforce_constant_time))
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
                            utils.into_iter().map(|v| v as f64).collect(),
                            enforce_constant_time))
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
                            utils.into_iter().map(|v| v as f64).collect(),
                            enforce_constant_time))
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
                            utils.into_iter().map(|v| v as f64).collect(),
                            enforce_constant_time))
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

impl Evaluable for proto::SnappingMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mut data = match get_argument(&arguments, "data")?.array()? {
            Array::F64(data) => data.clone(),
            Array::I64(data) => data.mapv(|v| v as f64),
            _ => return Err("data must be numeric".into())
        };

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;

        let epsilon = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.gencolumns().into_iter()))
            .zip(B.gencolumns().into_iter())
            .map(|((mut data_column, (sensitivity, epsilon)), B)| data_column.iter_mut()
                .zip(sensitivity.iter().zip(epsilon.iter()))
                .zip(B.iter())
                .map(|((v, (sens, eps)), B)| {
                    *v += utilities::mechanisms::snapping_mechanism(
                        &v, &eps, &self.b, &sens)?;
                    Ok(())
                })
                .collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true
        })
    }
}