use whitenoise_validator::errors::*;

use crate::{NodeArguments, execute_analysis};
use whitenoise_validator::base::{Array, ReleaseNode, Value, Jagged};
use whitenoise_validator::utilities::{get_argument, broadcast_privacy_usage, broadcast_ndarray, get_epsilon, get_delta};
use crate::components::Evaluable;
use crate::utilities;
use whitenoise_validator::proto;
use ndarray;
use ndarray::{Axis, arr1};
use whitenoise_validator::utilities::serial::parse_release_node;
use std::collections::HashMap;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mut data = match get_argument(&arguments, "data")?.array()? {
            Array::F64(data) => data.clone(),
            Array::I64(data) => data.mapv(|v| v as f64),
            _ => return Err("data must be numeric".into())
        };
//        println!("data: {:?}", data);

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;
//        println!("sensitivity: {:?}", sensitivity);

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;

        let epsilon = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;
//        println!("epsilon: {:?}", epsilon);

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

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::GaussianMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mut data = match get_argument(&arguments, "data")?.array()? {
            Array::F64(data) => data.clone(),
            Array::I64(data) => data.mapv(|v| v as f64),
            _ => return Err("data must be numeric".into())
        };
//        println!("data: {:?}", data.shape());

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;
//        println!("sensitivity: {:?}", sensitivity.shape());

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;

        let epsilon = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;
//        println!("epsilon: {:?}", epsilon.shape());

        let delta = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?)?;
//        println!("delta: {:?}", delta.shape());

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

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::SimpleGeometricMechanism {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let mut data = get_argument(&arguments, "data")?.array()?.i64()?.clone();
//        println!("data: {:?}", data.shape());

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?;
//        println!("sensitivity: {:?}", sensitivity.shape());

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;
//        println!("epsilon: {:?}", epsilon.shape());

        let lower = broadcast_ndarray(
            get_argument(&arguments, "lower")?.array()?.i64()?, data.shape())?;
//        println!("min: {:?}", min.shape());

        let upper = broadcast_ndarray(
            get_argument(&arguments, "upper")?.array()?.i64()?, data.shape())?;
//        println!("max: {:?}", max.shape());

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.gencolumns().into_iter()))
            .zip(lower.gencolumns().into_iter().zip(upper.gencolumns().into_iter()))
            .map(|((mut data_column, (sensitivity, epsilon)), (lower, upper))| data_column.iter_mut()
                .zip(sensitivity.iter().zip(epsilon.iter()))
                .zip(lower.iter().zip(upper.iter()))
                .map(|((v, (sens, eps)), (c_min, c_max))| {
                    *v += utilities::mechanisms::simple_geometric_mechanism(
                        &eps, &sens, &c_min, &c_max, &self.enforce_constant_time)?;
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
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(&arguments, "data")?;

        let sensitivity = get_argument(&arguments, "sensitivity")?.array()?.f64()?
            .iter().cloned().collect::<Vec<f64>>();

        let usages = broadcast_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let utility_proto = self.utility.as_ref().ok_or_else(|| "Utility must be defined")?;

        let value = match get_argument(&arguments, "candidates")?.jagged()? {
            Jagged::F64(candidates) => {
                let release_vec = candidates.iter().cloned().collect::<Option<Vec<Vec<f64>>>>()
                    .ok_or_else(|| "all candidates must be defined")?.into_iter()
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|(column, (sens, eps))| select_candidate(
                        &column.into_iter().map(Value::from).collect(),
                        eps, sens, data, &utility_proto)?.first_f64())
                    .collect::<Result<Vec<f64>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                // for some reason rustc needed a strange type hint
                Result::<Value>::Ok(release_array.into())
            },
            Jagged::I64(candidates) => {
                let release_vec = candidates.iter().cloned().collect::<Option<Vec<Vec<i64>>>>()
                    .ok_or_else(|| "all candidates must be defined")?.into_iter()
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|(column, (sens, eps))| select_candidate(
                        &column.into_iter().map(Value::from).collect(),
                        eps, sens, data, &utility_proto)?.first_i64())
                    .collect::<Result<Vec<i64>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Ok(release_array.into())
            },
            Jagged::Bool(candidates) => {
                let release_vec = candidates.iter().cloned().collect::<Option<Vec<Vec<bool>>>>()
                    .ok_or_else(|| "all candidates must be defined")?.into_iter()
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|(column, (sens, eps))| select_candidate(
                        &column.into_iter().map(Value::from).collect(),
                        eps, sens, data, &utility_proto)?.first_bool())
                    .collect::<Result<Vec<bool>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Ok(release_array.into())
            },
            Jagged::Str(candidates) => {
                let release_vec = candidates.iter().cloned().collect::<Option<Vec<Vec<String>>>>()
                    .ok_or_else(|| "all candidates must be defined")?.into_iter()
                    .zip(sensitivity.iter().zip(epsilon.iter()))
                    .map(|(column, (sens, eps))| select_candidate(
                        &column.into_iter().map(Value::from).collect(),
                        eps, sens, data, &utility_proto)?.first_string())
                    .collect::<Result<Vec<String>>>()?;

                let mut release_array = arr1(&release_vec).into_dyn();
                release_array.insert_axis_inplace(Axis(0));

                Ok(release_array.into())
            },
        }?;

        Ok(ReleaseNode {
            value,
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

fn select_candidate(
    candidates: &Vec<Value>,
    epsilon: &f64,
    sensitivity: &f64,
    data: &Value,
    utility_proto: &proto::Utility,
) -> Result<Value> {
    let utility_output_id = utility_proto.output_id;
    let mut utility_release = utility_proto.release.iter()
        .map(|(idx, release_node)| Ok((*idx, parse_release_node(release_node)?)))
        .collect::<Result<HashMap<u32, ReleaseNode>>>()?;
    utility_release.insert(utility_proto.dataset_id, ReleaseNode::new(data.clone()));

    let utility_analysis = proto::Analysis {
        privacy_definition: None,
        computation_graph: Some(proto::ComputationGraph { value: utility_proto.computation_graph.to_owned() }),
    };

    let utilities = candidates.iter()
        .map(|candidate| {
            utility_release.insert(utility_proto.candidate_id, ReleaseNode::new(candidate.clone()));
            let (release, _) = execute_analysis(
                &utility_analysis,
                utility_release.clone(),
                &proto::FilterLevel::All)?;

            release.get(&utility_output_id)
                .ok_or_else(|| Error::from("utility is undefined"))?
                .value.first_f64()
        })
        .collect::<Result<Vec<f64>>>()?;

    utilities::mechanisms::exponential_mechanism(
        epsilon, sensitivity, candidates, utilities)
}
