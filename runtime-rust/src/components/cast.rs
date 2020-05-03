use whitenoise_validator::errors::*;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, ReleaseNode};
use crate::components::Evaluable;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use crate::utilities::noise;
use whitenoise_validator::utilities::get_argument;


impl Evaluable for proto::Cast {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;
        match self.atomic_type.to_lowercase().as_str() {
            // if casting to bool, identify what value should map to true, then cast
            "bool" => {
                let true_label = get_argument(arguments, "true_label")?.array()?;
                Ok(cast_bool(&data, &true_label)?.into())
            },
            "float" | "real" => Ok(Value::Array(Array::F64(cast_f64(&data)?))),
            "int" | "integer" => {
                // TODO: handle different bounds on each column
                let lower = get_argument(arguments, "lower")?.first_i64()?;
                let upper = get_argument(arguments, "upper")?.first_i64()?;
                Ok(cast_i64(&data, &lower, &upper)?.into())
            },
            "string" | "str" =>
                Ok(cast_str(&data)?.into()),
            _ => Err("type is not recognized, must be BOOL, FLOAT, INT or STRING".into())
        }.map(ReleaseNode::new)
    }
}

/// Cast data to type `bool`.
///
/// The element(s) listed in `positive` are mapped to `true`, all others to `false`.
///
/// # Arguments
/// * `data` - Data to be cast to `bool`.
/// * `positive` - Positive class (class to be mapped to `true`) for each column.
///
/// # Return
/// Data cast to `bool`.
pub fn cast_bool(data: &Array, positive: &Array) -> Result<ArrayD<bool>> {
    fn compare<T: PartialEq + Clone>(data: &ArrayD<T>, label: &ArrayD<T>) -> Result<ArrayD<bool>> {
        let label = label.first()
            .ok_or_else(|| Error::from("label cannot be empty"))?;
        Ok(data.mapv(|v| v == *label))
    };

    match (data, positive) {
        (Array::Str(data), Array::Str(label)) => compare(&data, &label),
        (Array::Bool(data), Array::Bool(label)) => compare(&data, &label),
        (Array::I64(data), Array::I64(label)) => compare(&data, &label),
        (Array::F64(data), Array::F64(label)) => compare(&data, &label),
        _ => Err("data and positive class must share the same type".into())
    }
}

/// Cast data to type `f64`.
///
/// If data are `bool`, map `true => 1.` and `false => 0.`
///
/// If data are `String`, attempt to parse as `f64` and return `NAN` otherwise.
///
/// If data are `i64`, convert integers to their `f64` representation.
///
/// # Arguments
/// * `data` - Data to be cast to `f64`.
///
/// # Return
/// Data cast to `f64`.
pub fn cast_f64(data: &Array) -> Result<ArrayD<f64>> {
    Ok(match data {
        Array::Str(data) => data.mapv(|v| match v.parse::<f64>() {
            Ok(v) => v, Err(_) => std::f64::NAN
        }),
        Array::Bool(data) => data.mapv(|v| if v {1.} else {0.}),
        Array::I64(data) => data.mapv(|v| v as f64),
        Array::F64(data) => data.clone(),
    })
}

/// Cast data to type `i64`.
///
/// If data are `bool`, map `true => 1` and `false => 0`
///
/// If data are `String`, attempt to parse as `i64` and impute a uniform `i64` between `lower` and `upper` otherwise.
///
/// If data are `f64`, round non-`NAN` values to their `i64` representation,
/// impute uniform `i64` between `lower` and `upper` for values that are `NAN`.
///
/// # Arguments
/// * `data` - Data to be cast to `i64`.
/// * `lower` - Minimum allowable imputation value.
/// * `upper` - Maximum allowable imputation value.
///
/// # Return
/// Data cast to `i64`.
pub fn cast_i64(data: &Array, lower: &i64, upper: &i64) -> Result<ArrayD<i64>> {
    Ok(match data {
        Array::Str(data) => data
            .mapv(|v| v.parse::<i64>().unwrap_or_else(|_| noise::sample_uniform_int(&lower, &upper).unwrap())),
        Array::F64(data) => data
            .mapv(|v| if !v.is_nan() {v.round() as i64} else {noise::sample_uniform_int(&lower, &upper).unwrap()}),
        Array::Bool(data) => data.mapv(|v| if v {1} else {0}),
        Array::I64(data) => data.clone()
    })
}

/// Cast data to type `String`.
///
/// Regardless of data type, simply convert to `String`.
///
/// # Arguments
/// * `data` - Data to be cast to `String`.
///
/// # Return
/// Data cast to `String`.
pub fn cast_str(data: &Array) -> Result<ArrayD<String>> {
    Ok(match data {
        Array::Str(data) => data.clone(),
        Array::F64(data) => data.mapv(|v| v.to_string()),
        Array::Bool(data) => data.mapv(|v| v.to_string()),
        Array::I64(data) => data.mapv(|v| v.to_string())
    })
}