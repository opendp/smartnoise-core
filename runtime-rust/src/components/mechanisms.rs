use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND, get_argument};
use crate::components::Evaluable;
use crate::utilities;
use yarrow_validator::proto;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {

        println!("laplace mechanism {:?}", arguments);
        let epsilon: Vec<f64> = self.privacy_usage.iter().map(|usage| get_epsilon(&usage)).collect::<Result<Vec<f64>>>()?;
        let sensitivity = get_argument(&arguments, "sensitivity")?.get_arraynd()?.get_f64()?;

        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

        let mut data = data.clone();
        data.iter_mut()
            .zip(epsilon.iter())
            .zip(sensitivity.iter())
            .map(|((v, eps), sens)| {
                *v += utilities::mechanisms::laplace_mechanism(&eps, &sens)?;
                Ok(())
            })
            .collect::<Result<()>>()?;

        Ok(Value::ArrayND(ArrayND::F64(data)))
    }
}


impl Evaluable for proto::GaussianMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let epsilon: Vec<f64> = self.privacy_usage.iter().map(|usage| get_epsilon(&usage)).collect::<Result<Vec<f64>>>()?;
        let delta = get_argument(&arguments, "delta")?.get_arraynd()?.get_f64()?;
        let sensitivity = get_argument(&arguments, "sensitivity")?.get_arraynd()?.get_f64()?;

        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_f64()?;

        let mut data = data.clone();
        data.iter_mut()
            .zip(epsilon.iter())
            .zip(delta.iter())
            .zip(sensitivity.iter())
            .map(|(((v, eps), delta), sens)| {
                *v += utilities::mechanisms::gaussian_mechanism(&eps, &delta, &sens)?;
                Ok(())
            })
            .collect::<Result<()>>()?;

        Ok(Value::ArrayND(ArrayND::F64(data)))
    }
}

impl Evaluable for proto::SimpleGeometricMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let epsilon: Vec<f64> = self.privacy_usage.iter().map(|usage| get_epsilon(&usage)).collect::<Result<Vec<f64>>>()?;

        let sensitivity = get_argument(&arguments, "sensitivity")?.get_arraynd()?.get_f64()?;
        let count_min = get_argument(&arguments, "count_min")?.get_arraynd()?.get_i64()?;
        let count_max = get_argument(&arguments, "count_max")?.get_arraynd()?.get_i64()?;

        let enforce_constant_time = get_argument(&arguments, "enforce_constant_time")?.get_first_bool()?;

        let data = get_argument(&arguments, "data")?.get_arraynd()?.get_i64()?;

        let mut data = data.clone();
        data.iter_mut()
            .zip(epsilon.iter())
            .zip(count_min.iter().zip(count_max.iter()))
            .zip(sensitivity.iter())
            .map(|(((v, eps), (c_min, c_max)), sens)| {
                *v += utilities::mechanisms::simple_geometric_mechanism(
                    &eps, &sens, &c_min, &c_max, &enforce_constant_time)?;
                Ok(())
            })
            .collect::<Result<()>>()?;

        Ok(Value::ArrayND(ArrayND::I64(data)))
    }
}


fn get_epsilon(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone().ok_or::<Error>("distance must be defined on a PrivacyUsage".into())? {
        proto::privacy_usage::Distance::DistancePure(distance) => Ok(distance.epsilon),
        proto::privacy_usage::Distance::DistanceApproximate(distance) => Ok(distance.epsilon),
        _ => Err("epsilon is not defined".into())
    }
}

fn get_delta(usage: &proto::PrivacyUsage) -> Result<f64> {
    match usage.distance.clone().ok_or::<Error>("distance must be defined on a PrivacyUsage".into())? {
        proto::privacy_usage::Distance::DistanceApproximate(distance) => Ok(distance.delta),
        _ => Err("delta is not defined".into())
    }
}