use ndarray::{arr0};

use smartnoise_validator::{Float, Integer, proto};
use smartnoise_validator::base::{Array, ReleaseNode, Value};
use smartnoise_validator::errors::*;
use smartnoise_validator::utilities::{array::broadcast_ndarray, privacy::{get_delta, get_epsilon, spread_privacy_usage}, take_argument};

use crate::components::Evaluable;
use crate::NodeArguments;
use crate::utilities;
use crate::utilities::{get_num_columns, to_nd, get_num_rows};
use crate::utilities::mechanisms::exponential_mechanism;

impl Evaluable for proto::LaplaceMechanism {
    fn evaluate(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        mut arguments: NodeArguments
    ) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let mut data = take_argument(&mut arguments, "data")?.array()?.cast_float()?;
        let num_columns = get_num_columns(&data)?;
        let num_rows = get_num_rows(&data)?;

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.cast_float()?;
        let sens_num_columns = get_num_columns(&data)?;
        let sens_num_rows = get_num_rows(&data)?;
        if num_columns != sens_num_columns {
            return Err(Error::from(format!("data has {:?} columns, while the expected shape has {:?} columns. This is likely an error from substituting data into the graph.", num_columns, sens_num_columns)))
        }
        if num_rows != sens_num_rows {
            return Err(Error::from(format!("data has {:?} rows, while the expected shape has {:?} rows. This is likely an error from substituting data into the graph.", num_rows, sens_num_rows)))
        }
        if data.ndim() > 2 {
            return Err(Error::from("data may not have dimensionality greater than 2"))
        }


        let usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.into_iter()))
            .try_for_each(|(mut data_column, (sensitivity, epsilon))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .try_for_each(|(v, sens)|

                    utilities::mechanisms::laplace_mechanism(
                        epsilon, *sens as f64,
                        enforce_constant_time,
                    ).map(|noise| *v += noise as Float)))?;

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

        let mut data = take_argument(&mut arguments, "data")?.array()?.cast_float()?;
        let num_columns = get_num_columns(&data)?;
        let num_rows = get_num_rows(&data)?;

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.cast_float()?;
        let sens_num_columns = get_num_columns(&data)?;
        let sens_num_rows = get_num_rows(&data)?;
        if num_columns != sens_num_columns {
            return Err(Error::from(format!("data has {:?} columns, while the expected shape has {:?} columns. This is likely an error from substituting data into the graph.", num_columns, sens_num_columns)))
        }
        if num_rows != sens_num_rows {
            return Err(Error::from(format!("data has {:?} rows, while the expected shape has {:?} rows. This is likely an error from substituting data into the graph.", num_rows, sens_num_rows)))
        }
        if data.ndim() > 2 {
            return Err(Error::from("data may not have dimensionality greater than 2"))
        }

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;
        let delta = usages.iter().map(get_delta).collect::<Result<Vec<f64>>>()?;

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter())
            .zip(epsilon.into_iter().zip(delta.into_iter()))
            .try_for_each(|((mut data_column, sensitivity), (epsilon, delta))| data_column.iter_mut()
                .zip(sensitivity.iter())
                .try_for_each(|(v, sens)|

                    utilities::mechanisms::gaussian_mechanism(
                        epsilon, delta, *sens as f64, self.analytic,
                        enforce_constant_time,
                    ).map(|noise| *v += noise as Float)))?;

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

        let mut data = take_argument(&mut arguments, "data")?.array()?.int()?;
        let num_columns = get_num_columns(&data)?;
        let num_rows = get_num_rows(&data)?;

        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.int()?;
        let sens_num_columns = get_num_columns(&data)?;
        let sens_num_rows = get_num_rows(&data)?;
        if num_columns != sens_num_columns {
            return Err(Error::from(format!("data has {:?} columns, while the expected shape has {:?} columns. This is likely an error from substituting data into the graph.", num_columns, sens_num_columns)))
        }
        if num_rows != sens_num_rows {
            return Err(Error::from(format!("data has {:?} rows, while the expected shape has {:?} rows. This is likely an error from substituting data into the graph.", num_rows, sens_num_rows)))
        }
        if data.ndim() > 2 {
            return Err(Error::from("data may not have dimensionality greater than 2"))
        }

        let usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;
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
                .try_for_each(|((v, sens), (c_min, c_max))|

                    utilities::mechanisms::simple_geometric_mechanism(
                        epsilon, *sens as f64,
                        *c_min as i64, *c_max as i64,
                        enforce_constant_time,
                    ).map(|noise| *v += noise as Integer)))?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::ExponentialMechanism {
    fn evaluate(
        &self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments,
    ) -> Result<ReleaseNode> {
        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let candidates = take_argument(&mut arguments, "candidates")?.array()?;

        // exponential mechanism only works for single columns. Sensitivity will always be one value
        let sensitivity = take_argument(&mut arguments, "sensitivity")?.array()?.cast_float()?
            .iter().cloned().collect::<Vec<Float>>();

        let usages = spread_privacy_usage(&self.privacy_usage, sensitivity.len())?;
        let epsilon = usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?;

        let utilities = take_argument(&mut arguments, "utilities")?.array()?.cast_float()?;

        let num_columns = get_num_columns(&utilities)?;
        let num_rows = get_num_rows(&utilities)?;
        let cand_num_columns = candidates.num_columns()? as i64;
        let cand_num_rows = candidates.num_records()? as i64;
        if cand_num_columns != 1 {
            return Err(Error::from(format!("candidates has {:?} columns, but the exponential mechanism only works on single columns. This is likely an error from substituting data into the graph.", cand_num_columns)))
        }
        if num_columns != 1 {
            return Err(Error::from(format!("utilities has {:?} columns, but the exponential mechanism only works on single columns. This is likely an error from substituting data into the graph.", num_columns)))
        }
        if num_rows != cand_num_rows {
            return Err(Error::from(format!("utilities has {:?} rows, while the candidates has {:?} rows. This is likely an error from substituting data into the graph.", num_rows, cand_num_rows)))
        }
        if sensitivity.len() != 1 {
            return Err(Error::from(format!("sensitivity has length {:?}, but should have length one. This is likely an error from substituting data into the graph.", sensitivity.len())))
        }
        if utilities.ndim() > 2 {
            return Err(Error::from("utilities may not have dimensionality greater than 2"))
        }
        if candidates.shape().len() > 2 {
            return Err(Error::from("candidates may not have dimensionality greater than 2"))
        }

        macro_rules! apply_exponential {
            ($candidates:ident) => {
                {
                    let mut release_vec = $candidates.gencolumns().into_iter()
                        .zip(utilities.gencolumns().into_iter())
                        .zip(sensitivity.iter().zip(epsilon.iter()))
                        .map(|((cands, utils), (sens, eps))| exponential_mechanism(
                            *eps, *sens as f64,
                            &cands.to_vec(),
                            utils.into_iter().map(|v| *v as f64).collect(),
                            enforce_constant_time))
                        .collect::<Result<Vec<_>>>()?;

                    Value::from(arr0(release_vec.remove(0)).into_dyn())
                }
            }
        }

        Ok(ReleaseNode {
            value: match candidates {
                Array::Float(candidates) => apply_exponential!(candidates),
                Array::Int(candidates) => apply_exponential!(candidates),
                Array::Str(candidates) => apply_exponential!(candidates),
                Array::Bool(candidates) => apply_exponential!(candidates)
            },
            privacy_usages: Some(usages),
            public: true,
        })
    }
}

impl Evaluable for proto::SnappingMechanism {
    fn evaluate(&self, privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {

        let enforce_constant_time = privacy_definition.as_ref()
            .map(|v| v.protect_elapsed_time).unwrap_or(false);

        let mut data = take_argument(&mut arguments, "data")?
            .array()?.cast_float()?;
        let num_columns = get_num_columns(&data)?;
        let num_rows = get_num_rows(&data)?;

        let sensitivity = take_argument(&mut arguments, "sensitivity")?
            .array()?.cast_float()?;
        let sens_num_columns = get_num_columns(&data)?;
        let sens_num_rows = get_num_rows(&data)?;
        if num_columns != sens_num_columns {
            return Err(Error::from(format!("data has {:?} columns, while the expected shape has {:?} columns. This is likely an error from substituting data into the graph.", num_columns, sens_num_columns)))
        }
        if num_rows != sens_num_rows {
            return Err(Error::from(format!("data has {:?} rows, while the expected shape has {:?} rows. This is likely an error from substituting data into the graph.", num_rows, sens_num_rows)))
        }
        if data.ndim() > 2 {
            return Err(Error::from("data may not have dimensionality greater than 2"))
        }

        let usages = spread_privacy_usage(
            &self.privacy_usage, num_columns as usize)?;

        let epsilon = ndarray::Array::from_shape_vec(
            data.shape(), usages.iter().map(get_epsilon).collect::<Result<Vec<f64>>>()?)?;

        let num_columns = get_num_columns(&data)? as usize;

        let lower = to_nd(
            take_argument(&mut arguments, "lower")?.array()?.cast_float()?, 1)?
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();

        if num_columns != lower.len() {
            return Err("lower must share the same number of columns as data".into())
        }

        let upper = to_nd(
            take_argument(&mut arguments, "upper")?.array()?.cast_float()?, 1)?
            .into_dimensionality::<ndarray::Ix1>()?.to_vec();

        if num_columns != upper.len() {
            return Err("upper must share the same number of columns as data".into())
        }

        let binding_probability = match take_argument(&mut arguments, "binding_probability") {
            Ok(prob) => Some(prob.array()?.first_float()?),
            _ => None
        };

        data.gencolumns_mut().into_iter()
            .zip(sensitivity.gencolumns().into_iter().zip(epsilon.gencolumns().into_iter()))
            .zip(lower.into_iter().zip(upper.into_iter()))
            .try_for_each(|((mut data_column, (sensitivity, epsilon)), (lower, upper))| data_column.iter_mut()
                .zip(sensitivity.into_iter().zip(epsilon.into_iter()))
                .try_for_each(|(v, (sens, eps))|

                    utilities::mechanisms::snapping_mechanism(
                        *v, *eps, *sens as f64,
                        lower, upper, binding_probability,
                        enforce_constant_time
                    ).map(|privatized| *v = privatized as Float)))?;

        Ok(ReleaseNode {
            value: data.into(),
            privacy_usages: Some(usages),
            public: true
        })
    }
}