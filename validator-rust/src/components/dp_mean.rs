use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Accuracy, Privatize, Expandable, Report};
use ndarray::{Array, arr1};
use crate::utilities::serial::serialize_value;
use crate::base::{Properties, NodeProperties, Value, get_constant, ArrayND};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};

use serde_json;

impl Component for proto::DpMean {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or("data argument missing from DPMean")?.clone();

        // check that all properties are satisfied
        data_property.get_n()?;
        data_property.get_min_f64()?;
        data_property.get_max_f64()?;
        data_property.assert_non_null()?;

        data_property.num_records = (0..data_property.num_columns.unwrap()).map(|_| Some(1)).collect();
        data_property.releasable = true;

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::DpMean {
    fn expand_graph(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<(u32, HashMap<u32, proto::Component>)> {
        let mut current_id = maximum_id.clone();
        let mut graph_expansion: HashMap<u32, proto::Component> = HashMap::new();

        // mean
        current_id += 1;
        let id_mean = current_id.clone();
        graph_expansion.insert(id_mean, proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").unwrap()],
            value: Some(proto::component::Value::Mean(proto::Mean {})),
            omit: true,
            batch: component.batch,
        });

        let sensitivity = Value::ArrayND(ArrayND::F64(Array::from(component.value.to_owned().unwrap()
            .compute_sensitivity(privacy_definition, properties)
            .unwrap()).into_dyn()));

        // sensitivity literal
        current_id += 1;
        let id_sensitivity = current_id.clone();
        graph_expansion.insert(id_sensitivity, get_constant(&sensitivity, &component.batch));


        let epsilon = Value::ArrayND(ArrayND::F64(Array::from(match self.privacy_usage.clone().unwrap().usage.unwrap() {
            proto::privacy_usage::Usage::DistancePure(distance) => vec![distance.epsilon],
            proto::privacy_usage::Usage::DistanceApproximate(distance) => vec![distance.epsilon],
        }).into_dyn()));
        // epsilon literal
        current_id += 1;
        let id_epsilon = current_id.clone();
        graph_expansion.insert(id_epsilon, get_constant(&epsilon, &component.batch));

        // noising
        graph_expansion.insert(component_id, proto::Component {
            arguments: hashmap!["data".to_owned() => id_mean, "sensitivity".to_owned() => id_sensitivity, "epsilon".to_owned() => id_epsilon],
            value: Some(proto::component::Value::LaplaceMechanism(proto::LaplaceMechanism {
                privacy_usage: self.privacy_usage.clone()
            })),
            omit: true,
            batch: component.batch,
        });

        Ok((current_id, graph_expansion))
    }
}

impl Privatize for proto::DpMean {
    fn compute_sensitivity(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
    ) -> Option<Vec<f64>> {
        let data_property = properties.get("data")?;

        let min = data_property.get_min_f64().ok()?;
        let max = data_property.get_max_f64().ok()?;
        let num_records = data_property.get_n().ok()?;

        Some(min
            .iter()
            .zip(max)
            .zip(num_records)
            .map(|((min, max), n)| (max - min) / n as f64)
            .collect())
    }
}

impl Accuracy for proto::DpMean {
    fn accuracy_to_privacy_usage(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _properties: &base::NodeProperties,
        _accuracy: &proto::Accuracy,
    ) -> Option<proto::PrivacyUsage> {
        None
    }

    fn privacy_usage_to_accuracy(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _property: &base::NodeProperties,
    ) -> Option<f64> {
        None
    }
}

<<<<<<< Updated upstream
impl Report for proto::DpMean {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        properties: &NodeProperties,
        release: &Value
    ) -> Option<Vec<JSONRelease>> {

=======
/// returns JSON Schema for DpMean
/// example: schema is an array of 2 elements ( for dp mean release)
>>>>>>> Stashed changes
//    let mut schema = vec![JSONRelease {
//        description: "".to_string(),
//        variables: vec![],
//        statistics: "dpmean".to_string(),
//        releaseInfo: Default::default(),
//        privacyLoss: PrivacyLoss::Pure(PureLoss { epsilon: 0.5 }),
//        accuracy: None,
//        batch: 0,
//        nodeID: 0,
//        postprocess: false,
//        algorithmInfo: AlgorithmInfo {
//            name: "Laplace".to_string(),
//            cite: "haghsg".to_string(),
//            argument: HashMap::new(),
//        },
//    },
//    JSONRelease {
//        description: "".to_string(),
//        variables: vec![],
//        statistics: "dpmean".to_string(),
//        releaseInfo: Default::default(),
//        privacyLoss: PrivacyLoss::Concentrated(Concentrated { rho: 0.4 }),
//        accuracy: None,
//        batch: 0,
//        nodeID: 0,
//        postprocess: true,
//        algorithmInfo: AlgorithmInfo {
//            name: "histogram".to_string(),
//            cite: "...".to_string(),
//            argument: HashMap::new(),
//        },
//    }];

<<<<<<< Updated upstream
        let data_properties: &Properties = properties.get("data").unwrap();
=======
impl Report for proto::DpMean {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>> {



        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

>>>>>>> Stashed changes
        let mut releases = Vec::new();

        let minimums = data_properties.get_min_f64().unwrap();
        let maximums = data_properties.get_max_f64().unwrap();
        let num_records = data_properties.get_n().unwrap();

        for column_number in (0..data_properties.num_columns.unwrap()) {

            let mut releaseInfo = HashMap::new();
            releaseInfo.insert("mechanism".to_string(), serde_json::json!(self.implementation.clone()));
            releaseInfo.insert("releaseValue".to_string(), value_to_json(&release).unwrap());

            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPMean".to_string(),
                variables: vec![],
                releaseInfo,
                privacyLoss: privacy_usage_to_json(&self.privacy_usage.clone().unwrap()),
                accuracy: None,
                batch: component.batch as u64,
                nodeID: node_id.clone() as u64,
                postprocess: false,
                algorithmInfo: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    argument: serde_json::json!({
                        "n": num_records[column_number as usize],
                        "constraint": {
                            "lowerbound": minimums[column_number as usize],
                            "upperbound": maximums[column_number as usize]
                        }
                    })
                }
            };

            releases.push(release);
        }
        Some(releases)

    }
}
