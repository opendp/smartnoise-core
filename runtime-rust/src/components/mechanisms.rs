use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value};
use whitenoise_validator::utilities::{get_argument, broadcast_privacy_usage, broadcast_ndarray};
use crate::components::Evaluable;
use crate::utilities;
use whitenoise_validator::proto;
use ndarray::Array;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let mut data = get_argument(&arguments, "data")?.array()?.f64()?.clone();
        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;

        let epsilon = Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.gencolumns().into_iter()))
            .map(|(mut data_column, (sensitivity, epsilon))| data_column.iter_mut()
                .zip(sensitivity.iter().zip(epsilon.iter()))
                .map(|(v, (sens, eps))| {
                    *v += utilities::mechanisms::laplace_mechanism(&eps, &sens)?;
                    Ok(())
                })
                .collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(data.into())
    }
}

impl Evaluable for proto::GaussianMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let mut data = get_argument(&arguments, "data")?.array()?.f64()?.clone();
        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;

        let epsilon = Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;

        let delta = Array::from_shape_vec(
            data.shape(), usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?)?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter())
            .zip(epsilon.gencolumns().into_iter().zip(delta.gencolumns().into_iter()))
            .map(|((mut data_column, sensitivity), (epsilon, delta))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .zip(epsilon.iter().zip(delta.iter()))
                .map(|((v, sens), (eps, del))| {
                    *v += utilities::mechanisms::gaussian_mechanism(&eps, &del, &sens)?;
                    Ok(())
                }).collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(data.into())
    }
}

impl Evaluable for proto::SimpleGeometricMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let mut data = get_argument(&arguments, "data")?.array()?.i64()?.clone();
//        println!("data: {:?}", data.shape());

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;
//        println!("sensitivity: {:?}", sensitivity.shape());

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;
//        println!("epsilon: {:?}", epsilon.shape());

        let count_min = broadcast_ndarray(
            get_argument(&arguments, "count_min")?.array()?.i64()?, data.shape())?;
//        println!("count_min: {:?}", count_min.shape());

        let count_max = broadcast_ndarray(
            get_argument(&arguments, "count_max")?.array()?.i64()?, data.shape())?;
//        println!("count_max: {:?}", count_max.shape());

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.gencolumns().into_iter()))
            .zip(count_min.gencolumns().into_iter().zip(count_max.gencolumns().into_iter()))
            .map(|((mut data_column, (sensitivity, epsilon)), (count_min, count_max))| data_column.iter_mut()
                .zip(sensitivity.iter().zip(epsilon.iter()))
                .zip(count_min.iter().zip(count_max.iter()))
                .map(|((v, (sens, eps)), (c_min, c_max))| {
                    *v += utilities::mechanisms::simple_geometric_mechanism(
                        &eps, &sens, &c_min, &c_max, &self.enforce_constant_time)?;
                    Ok(())
                })
                .collect::<Result<()>>())
            .collect::<Result<()>>()?;

        Ok(data.into())
    }
}


fn get_epsilon(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::DistancePure(distance) => Ok(distance.epsilon),
        proto::privacy_usage::Distance::DistanceApproximate(distance) => Ok(distance.epsilon),
//        _ => Err("epsilon is not defined".into())
    }
}

fn get_delta(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone()
        .ok_or_else(|| Error::from("distance must be defined on a PrivacyUsage"))? {
        proto::privacy_usage::Distance::DistanceApproximate(distance) => Ok(distance.delta),
        _ => Err("delta is not defined".into())
    }
}