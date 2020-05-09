//! Representation for report/json summaries

use crate::errors::*;
use serde::{Deserialize, Serialize};
extern crate serde_json;

use crate::proto;
use crate::base;

use serde_json::Value;
use ndarray::prelude::*;


/// JSONRelease represents JSON objects in the differential privacy release schema.
/// TODO: link to schema
#[derive(Serialize, Deserialize)]
pub struct JSONRelease {
    pub description: String,
    /// array of string that is column/s in the dataset
    pub variables: Value,
    /// User provide a value for either epsilon (epsilon>0), delta (0<delta<1>), or rho depending on the type of dp definitions (i.e. approximate or concentrated).
    pub statistic: String,
    /// The value released by the system
    #[serde(rename(serialize = "releaseInfo", deserialize = "releaseInfo"))]
    pub release_info: Value,
    /// The amount of privacy used to compute the release value
    #[serde(rename(serialize = "privacyLoss", deserialize = "privacyLoss"))]
    pub privacy_loss: Value,
    /// optional parameter. It is a combination of the accuracy and alpha value
    pub accuracy: Option<Accuracy>,
    /// which release the implemented statistic is originating from. This provides a tool to keep track of overall privacyLoss.
    pub batch: u64,
    /// For advanced users. Corresponds to the node of the graph this release originated from
    #[serde(rename(serialize = "nodeID", deserialize = "nodeID"))]
    pub node_id: u64,
    /// true when the released value is derived from public/released data
    pub postprocess: bool,
    /// the name of the algorithm which is implemented for computation of the given statistic and the arguments of the algorithm such as n(number of observations),  range (upper and lower bound, etc.)
    #[serde(rename(serialize = "algorithmInfo", deserialize = "algorithmInfo"))]
    pub algorithm_info: AlgorithmInfo,
}

/// Statistical accuracy summary
///
/// The actual value refers to the non-privatized statistic on sample data, not the non-privatized statistic of the population
#[derive(Serialize, Deserialize)]
pub struct Accuracy {
    /// Upper bound on the distance between the estimated value and actual value.
    #[serde(rename(serialize = "accuracyValue", deserialize = "accuracyValue"))]
    pub accuracy_value: f64,
    /// 100(1 - alpha)% confidence that the actual value is within the interval spanned by the accuracyValue.
    pub alpha: f64,
}

/// Algorithm summary
///
/// Metadata about the algorithm used to compute the release value.
#[derive(Serialize, Deserialize)]
pub struct AlgorithmInfo {
    // mechanism used to generate the release values, typically `Laplace`, `Exponential`, etc.
    pub mechanism: String,
    pub name: String,
    /// Citation to originating paper
    pub cite: String,
    /// The arguments of the algorithm such as n (number of observations),  range (upper and lower bound, etc.).
    pub argument: Value,
}

/// converts an ArrayND (which can take any of types (float, integer, string, and Boolean) to JSON
pub fn value_to_json(value: &base::Value) -> Result<serde_json::Value> {
    match value {
        base::Value::Array(array) => match array {
            base::Array::F64(value) => arraynd_to_json(value),
            base::Array::I64(value) => arraynd_to_json(value),
            base::Array::Str(value) => arraynd_to_json(value),
            base::Array::Bool(value) => arraynd_to_json(value)
        },
        _ => Err("only arrayND to json is implemented".into())
    }
}

/// Converts n dimensional array to json arrays
pub fn arraynd_to_json<T: Serialize + Clone>(array: &ArrayD<T>) -> Result<serde_json::Value> {
    match array.ndim() {
        0 => Ok(serde_json::json!(array.first().unwrap())),
        1 => Ok(serde_json::json!(array.clone().into_dimensionality::<Ix1>()?.to_vec())),
        2 => Ok(serde_json::json!(array.genrows().into_iter().map(|row| row.iter().cloned().collect::<Vec<T>>()).collect::<Vec<Vec<T>>>())),
        _ => Err("array must have dimensionality less than 2".into())
    }
}

/// Converts the prost Protobuf PrivacyLoss into a json representation.
///
/// User provide a value for either epsilon, delta, or rho depending on the type of dp definitions (i.e. approximate and concentrated).
pub fn privacy_usage_to_json(privacy_usage: &proto::PrivacyUsage) -> serde_json::Value {
    match privacy_usage.distance.clone().unwrap() {
        proto::privacy_usage::Distance::Approximate(distance) =>
            serde_json::json!({"name": "approximate", "epsilon": distance.epsilon, "delta": distance.delta})
    }
}
