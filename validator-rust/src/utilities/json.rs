/// ***JSON Release*** ///

use crate::errors::*;
use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};
extern crate serde_json;

use crate::proto;
use crate::base;

use serde_json::Value;
use ndarray::prelude::*;

/// "struct JSONRelease" represents JSON objects.
/// "description" is a string that describes the release.
/// "variables" is an array of string that is column/s in the dataset.
/// statistics is a string, it shows which statistic is implemented (e.g. mean, histogram, variance, etc.)
/// "privacyLoss" is a Value type, Value is either an ArrayND, HashMap, or Vector2DJagged (for jagged matrices). User provide a value for either epsilon (epsilon>0), delta (0<delta<1>), or rho depending on the type of dp definitions (i.e. pure, approximated and concerted).
/// "batch" is a numeric Which shows from which release the implemented statistic is originating from. This provides a tool to keep track of overall privacyLoss.
/// "nodeID" is a numeric value, it shows the release corresponds to which node of the graph.
/// "postprocess" is a Boolean, it is true when there are more than one variables.
/// algorithmInfo returns the information about the algorithm and its parameters.
/// "Accuracy" is an optional parameter. It is a combination of the accuracy and alpha value.
/// "AlgorithmInfo" includes: the name of the algorithm which is implemented for computation of the given statistic and the arguments of the algorithm such as n(number of observations),  range (upper and lower bound, etc.)


#[derive(Serialize, Deserialize)]
pub struct JSONRelease {
    pub description: String,
    pub variables: Vec<String>,
    pub statistic: String,
    pub releaseInfo: HashMap<String, Value>,
    pub privacyLoss: Value,
    pub accuracy: Option<Accuracy>,
    pub batch: u64,
    pub nodeID: u64,
    pub postprocess: bool,
    pub algorithmInfo: AlgorithmInfo,
}


#[derive(Serialize, Deserialize)]
pub struct Accuracy {
    pub accuracyValue: f64,
    pub alpha: f64,
}


#[derive(Serialize, Deserialize)]
pub struct AlgorithmInfo {
    pub name: String,
    pub cite: String,
    pub argument: Value,
}

/// All components take in a HashMap<String, Value> and return a Value. A Value is either an ArrayND, HashMap, or Vector2DJagged (for jagged matrices).
///A component is the smallest unit of computation in this framework. It represents some computation- Load a table (Materialize), extract a column from a table (Index), add two columns (Add), impute missing values (Impute), compute the mean of a column (Mean), add Laplacian noise, (LaplaceMechanism) etc.
/// "value_to_json" function converts an ArrayND (which can take either of types (float, integer, string, and Boolean) to JSON via the “arraynd_to_json“  function.

pub fn value_to_json(value: &base::Value) -> Result<serde_json::Value> {
    match value {
        base::Value::ArrayND(array) => match array {
            base::ArrayND::F64(value) => arraynd_to_json(value),
            base::ArrayND::I64(value) => arraynd_to_json(value),
            base::ArrayND::Str(value) => arraynd_to_json(value),
            base::ArrayND::Bool(value) => arraynd_to_json(value)
        },
        _ => Err("only arrayND to json is implemented".into())
    }
}

/// "arraynd_to_json" function converts Value to JSON. This function is required since the output of components is a Value not an ArrayND

/// This function can be called  on arrays of any type "arrayD<T>" (T is a generic type for array).
///converting n dimensional array to a vector, and converting to serde_json value which is the 2-dimensional value.
pub fn arraynd_to_json<T: Serialize + Clone>(array: &ArrayD<T>) -> Result<serde_json::Value> {
    match array.ndim() {
        0 => Ok(serde_json::json!(array.first().unwrap())),
        1 => Ok(serde_json::json!(array.clone().into_dimensionality::<Ix1>().unwrap().to_vec())),
//        2 => {
//            serde_json::json!(array.into_dimensionality::<Ix2>().clone().unwrap().to_vec())
//        },
        _ => Err("converting a matrix to json is not implemented".into())
    }
}

/// "privacy_usage_to_json" returns PrivacyLoss Value. User provide a value for either epsilon, delta, or rho depending on the type of dp definitions (i.e. pure, approximated and concerted).

pub fn privacy_usage_to_json(privacy_usage: &proto::PrivacyUsage) -> serde_json::Value {
    match privacy_usage.usage.clone().unwrap() {
        proto::privacy_usage::Usage::DistancePure(distance) =>
            serde_json::json!({"name": "pure", "epsilon": distance.epsilon}),
        proto::privacy_usage::Usage::DistanceApproximate(distance) =>
            serde_json::json!({"name": "approximate", "epsilon": distance.epsilon, "delta": distance.delta})
    }
}
