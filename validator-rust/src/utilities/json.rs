
use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};
//use schemars::{schema_for, JsonSchema};
extern crate serde_json;
use serde_json:: Value;
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

pub struct PureLoss {
    epsilon: f64
}
pub struct Approx {
    epsilon: f64,
    delta:f64
}
pub struct Concentrated {
    rho: f64
}
pub enum PrivacyLoss {
    Pure(PureLoss),
    Approximate(Approx),
    concentrated(Concentrated)
}
pub struct Accuracy {
    accuracyValue: f64,
    alpha: f64
}
pub struct AlgorithmInfo {
    name : String,
    cite:String,
    argurment: HashMap<String, Value>
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
}
