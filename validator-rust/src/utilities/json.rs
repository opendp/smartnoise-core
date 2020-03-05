use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};

//use schemars::{schema_for, JsonSchema};
//extern crate json_typegen;
extern crate serde_json;
use crate::proto;
use crate::base;

//use json_typegen::json_typegen;
use serde_json::Value;

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

pub fn value_to_json(value: &base::Value) -> serde_json::Value {
    serde_json::json!(1)
}


pub fn privacy_usage_to_json(privacy_usage: &proto::PrivacyUsage) -> serde_json::Value {
    match privacy_usage.usage.clone().unwrap() {
        proto::privacy_usage::Usage::DistancePure(distance) =>
            serde_json::json!({"name": "pure", "epsilon": distance.epsilon}),
        proto::privacy_usage::Usage::DistanceApproximate(distance) =>
            serde_json::json!({"name": "approximate", "epsilon": distance.epsilon, "delta": distance.delta})
    }
}