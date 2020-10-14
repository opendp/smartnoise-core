use smartnoise_validator::errors::*;

use crate::NodeArguments;
use smartnoise_validator::base::{Value, Array, ReleaseNode};
use crate::components::Evaluable;
use ndarray::ArrayD;
use smartnoise_validator::{proto, Float, Integer};
use crate::utilities::noise;
use smartnoise_validator::utilities::take_argument;


impl Evaluable for proto::Cast {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, mut arguments: NodeArguments) -> Result<ReleaseNode> {
        let data = take_argument(&mut arguments, "data")?.array()?;
        match self.atomic_type.to_lowercase().as_str() {
            // if casting to bool, identify what value should map to true, then cast
            "bool" => {
                let true_label = take_argument(&mut arguments, "true_label")?.array()?;
                Ok(cast_bool(&data, &true_label)?.into())
            },
            "float" | "real" => Ok(Value::Array(Array::Float(cast_float(&data)?))),
            "int" | "integer" => {
                // TODO: handle different bounds on each column
                let lower = take_argument(&mut arguments, "lower")?.array()?.first_int()?;
                let upper = take_argument(&mut arguments, "upper")?.array()?.first_int()?;
                Ok(cast_int(&data, lower, upper)?.into())
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
        (Array::Int(data), Array::Int(label)) => compare(&data, &label),
        (Array::Float(data), Array::Float(label)) => compare(&data, &label),
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
/// Data cast to `Float`.
pub fn cast_float(data: &Array) -> Result<ArrayD<Float>> {
    Ok(match data {
        Array::Str(data) => data.mapv(|v| match v.parse::<Float>() {
            Ok(v) => v, Err(_) => Float::NAN
        }),
        Array::Bool(data) => data.mapv(|v| if v {1.} else {0.}),
        Array::Int(data) => data.mapv(|v| v as Float),
        Array::Float(data) => data.clone(),
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
pub fn cast_int(data: &Array, lower: Integer, upper: Integer) -> Result<ArrayD<Integer>> {
    Ok(match data {
        Array::Str(data) => data
            .mapv(|v| v.parse::<Integer>().unwrap_or_else(|_| noise::sample_uniform_int(lower, upper).unwrap())),
        Array::Float(data) => data
            .mapv(|v| if !v.is_nan() {v.round() as Integer} else {noise::sample_uniform_int(lower, upper).unwrap()}),
        Array::Bool(data) => data.mapv(|v| if v {1} else {0}),
        Array::Int(data) => data.clone()
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
        Array::Float(data) => data.mapv(|v| v.to_string()),
        Array::Bool(data) => data.mapv(|v| v.to_string()),
        Array::Int(data) => data.mapv(|v| v.to_string())
    })
}