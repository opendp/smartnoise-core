use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};

//use schemars::{schema_for, JsonSchema};
//extern crate json_typegen;
extern crate serde_json;
//use json_typegen::json_typegen;
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct JSONRelease {
    pub description: String,
    pub variables: Vec<String>,
    pub statistics: String,
    pub releaseInfo: HashMap<String, Value>,
    pub privacyLoss: PrivacyLoss,
    pub accuracy: Option<Accuracy>,
    pub batch: u64,
    pub nodeID: u64,
    pub postprocess: bool,
    pub algorithmInfo: AlgorithmInfo,
}

#[derive(Serialize, Deserialize)]
pub struct PureLoss {
    pub epsilon: f64
}

#[derive(Serialize, Deserialize)]
pub struct Approx {

    pub epsilon: f64,
    pub delta:f64
}

#[derive(Serialize, Deserialize)]
pub struct Concentrated {
    pub rho: f64
}

#[derive(Serialize, Deserialize)]
pub enum PrivacyLoss {
    Pure(PureLoss),
    Approximate(Approx),
    concentrated(Concentrated),
}

#[derive(Serialize, Deserialize)]
pub struct Accuracy {

    pub accuracyValue: f64,
    pub alpha: f64

}

#[derive(Serialize, Deserialize)]
pub struct AlgorithmInfo {

    pub name : String,
    pub cite:String,
    pub argument:HashMap<String, Value>
}
