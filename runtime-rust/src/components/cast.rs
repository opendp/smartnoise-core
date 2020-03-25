use whitenoise_validator::errors::*;

use crate::base::NodeArguments;
use whitenoise_validator::base::{Value, ArrayND};
use crate::components::Evaluable;
use ndarray::ArrayD;
use whitenoise_validator::proto;
use crate::utilities::noise;
use whitenoise_validator::utilities::get_argument;


impl Evaluable for proto::Cast {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<Value> {
        let output_type = get_argument(&arguments, "type")?.get_first_str()?;

        let data = get_argument(&arguments, "data")?.get_arraynd()?;
        match output_type.to_lowercase().as_str() {
            // if casting to bool, identify what value should map to true, then cast
            "bool" => {
                let true_label = get_argument(&arguments, "true_label")?.get_arraynd()?;
                Ok(cast_bool(&data, &true_label)?.into())
            },
            "float" | "real" => Ok(Value::ArrayND(ArrayND::F64(cast_f64(&data)?))),
            "int" | "integer" => {
                // TODO: handle different bounds on each column
                let min = get_argument(&arguments, "min")?.get_first_i64()?;
                let max = get_argument(&arguments, "max")?.get_first_i64()?;
                Ok(cast_i64(&data, &min, &max)?.into())
            },
            "string" | "str" =>
                Ok(cast_str(&data)?.into()),
            _ => Err("type is not recognized, must be BOOL, FLOAT, INT or STRING".into())
        }
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
pub fn cast_bool(data: &ArrayND, positive: &ArrayND) -> Result<ArrayD<bool>> {
    fn compare<T: PartialEq + Clone>(data: &ArrayD<T>, label: &ArrayD<T>) -> Result<ArrayD<bool>> {
        let label = label.first()
            .ok_or::<Error>("label cannot be empty".into())?;
        Ok(data.mapv(|v| v == *label))
    };

    match (data, positive) {
        (ArrayND::Str(data), ArrayND::Str(label)) => compare(&data, &label),
        (ArrayND::Bool(data), ArrayND::Bool(label)) => compare(&data, &label),
        (ArrayND::I64(data), ArrayND::I64(label)) => compare(&data, &label),
        (ArrayND::F64(data), ArrayND::F64(label)) => compare(&data, &label),
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
pub fn cast_f64(data: &ArrayND) -> Result<ArrayD<f64>> {
    Ok(match data {
        ArrayND::Str(data) => data.mapv(|v| match v.parse::<f64>() {
            Ok(v) => v, Err(_) => std::f64::NAN
        }),
        ArrayND::Bool(data) => data.mapv(|v| if v {1.} else {0.}),
        ArrayND::I64(data) => data.mapv(|v| v as f64),
        ArrayND::F64(data) => data.clone(),
    })
}

/// Cast data to type `i64`.
///
/// If data are `bool`, map `true => 1` and `false => 0`
///
/// If data are `String`, attempt to parse as `i64` and impute a uniform `i64` between `min` and `max` otherwise.
///
/// If data are `f64`, round non-`NAN` values to their `i64` representation,
/// impute uniform `i64` between `min` and `max` for values that are `NAN`.
///
/// # Arguments
/// * `data` - Data to be cast to `i64`.
/// * `min` - Minimum allowable imputation value.
/// * `max` - Maximum allowable imputation value.
///
/// # Return
/// Data cast to `i64`.
pub fn cast_i64(data: &ArrayND, min: &i64, max: &i64) -> Result<ArrayD<i64>> {
    Ok(match data {
        ArrayND::Str(data) => data
            .mapv(|v| v.parse::<i64>().unwrap_or_else(|_| noise::sample_uniform_int(&min, &max).unwrap())),
        ArrayND::F64(data) => data
            .mapv(|v| if !v.is_nan() {v.round() as i64} else {noise::sample_uniform_int(&min, &max).unwrap()}),
        ArrayND::Bool(data) => data.mapv(|v| if v {1} else {0}),
        ArrayND::I64(data) => data.clone()
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
pub fn cast_str(data: &ArrayND) -> Result<ArrayD<String>> {
    Ok(match data {
        ArrayND::Str(data) => data.clone(),
        ArrayND::F64(data) => data.mapv(|v| v.to_string()),
        ArrayND::Bool(data) => data.mapv(|v| v.to_string()),
        ArrayND::I64(data) => data.mapv(|v| v.to_string())
    })
}