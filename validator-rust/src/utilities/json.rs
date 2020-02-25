
use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};
//use schemars::{schema_for, JsonSchema};
extern crate serde_json;
use serde_json:: Value;
#[derive(Serialize, Deserialize)]
pub struct JSONRelease {
    pub description: String,
    pub variables: Vec<String>,
    pub statistics: String,
    pub releaseInfo: HashMap<String, Value>,
    pub privacyLoss: PrivacyLoss,
    pub accuracy: Option<Accuracy>,
    pub batch:i64,
    pub nodeID:i64,
    pub postprocess:bool,
    pub algorithmInfo:AlgorithmInfo
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
    concentrated(Concentrated)
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
    pub argurment: HashMap<String, Value>
}

fn main(){
    let mut schema= JSONRelease{
    description: "".to_string(),
    variables: vec![],
    statistics: "".to_string(),
    releaseInfo: Default::default(),
    privacyLoss: PrivacyLoss::Pure(PureLoss{ epsilon:0.5}),
    accuracy: None,
    batch: 0,
    nodeID: 0,
    postprocess: false,
    algorithmInfo:AlgorithmInfo{
        name:"Laplace".to_string(),
        cite:"haghsg".to_string(),
        argurment: HashMap::new()
    }
};
let j= serde_json::to_string(&schema).unwrap();
println!("schema is: {}",j)
}
