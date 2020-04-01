use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, broadcast_privacy_usage, get_ith_release};


impl Component for proto::DpHistogram {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPHistogram is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        arg_vars: HashMap<String, Vec<String>>,
    ) -> Result<Vec<String>> {
        return Ok(arg_vars.values().cloned().flatten().collect::<Vec<String>>());
    }
}


impl Expandable for proto::DpHistogram {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = maximum_id.clone();
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        let mut data_id = component.arguments.get("data")
            .ok_or::<Error>("data is a required argument to DPHistogram".into())?.clone();

        let mut traversal = Vec::<u32>::new();
        match (component.arguments.get("edges"), component.arguments.get("categories")) {

            (Some(edges_id), None) => {
                // digitize
                let null_id = component.arguments.get("null")
                    .ok_or::<Error>("null is a required argument to DPHistogram".into())?;
                let inclusive_left_id = component.arguments.get("inclusive_left")
                    .ok_or::<Error>("inclusive_left is a required argument to DPHistogram when categories are not known".into())?;
                current_id += 1;
                let id_digitize = current_id.clone();
                computation_graph.insert(id_digitize, proto::Component {
                    arguments: hashmap![
                        "data".to_owned() => data_id,
                        "edges".to_owned() => *edges_id,
                        "null".to_owned() => *null_id,
                        "inclusive_left".to_owned() => *inclusive_left_id
                    ],
                    variant: Some(proto::component::Variant::from(proto::Digitize {})),
                    omit: true,
                    batch: component.batch,
                });
                data_id = id_digitize.clone();
                traversal.push(id_digitize.clone());
            }

            (None, Some(categories_id)) => {
                // clamp
                let null_id = component.arguments.get("null")
                    .ok_or::<Error>("null is a required argument to DPHistogram when categories are not known".into())?;
                current_id += 1;
                let id_clamp = current_id.clone();
                computation_graph.insert(id_clamp, proto::Component {
                    arguments: hashmap![
                        "data".to_owned() => data_id,
                        "categories".to_owned() => *categories_id,
                        "null".to_owned() => *null_id
                    ],
                    variant: Some(proto::component::Variant::from(proto::Clamp {})),
                    omit: true,
                    batch: component.batch,
                });
                data_id = id_clamp.clone();
                traversal.push(id_clamp.clone());
            }

            (None, None) => {
                let data_property = properties.get("data")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                if data_property.categories().is_err() {
                    return Err("either edges or categories must be supplied".into())
                }
            }
            _ => return Err("either edges or categories must be supplied".into())
        }

        // histogram
        current_id += 1;
        let id_histogram = current_id.clone();
        computation_graph.insert(id_histogram.clone(), proto::Component {
            arguments: hashmap!["data".to_owned() => data_id],
            variant: Some(proto::component::Variant::from(proto::Histogram {})),
            omit: true,
            batch: component.batch,
        });
        traversal.push(id_histogram);

        // noising
        computation_graph.insert(component_id.clone(), proto::Component {
            arguments: hashmap![
                "data".to_owned() => id_histogram,
                "count_min".to_owned() => *component.arguments.get("count_min").ok_or::<Error>("count_min must be provided as an argument".into())?,
                "count_max".to_owned() => *component.arguments.get("count_max").ok_or::<Error>("count_max must be provided as an argument".into())?
            ],
            variant: Some(proto::component::Variant::from(proto::SimpleGeometricMechanism {
                privacy_usage: self.privacy_usage.clone(),
                enforce_constant_time: false
            })),
            omit: false,
            batch: component.batch,
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal
        })
    }
}

impl Report for proto::DpHistogram {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        release: &Value,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..num_columns {
            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPHistogram".to_string(),
                variables: serde_json::json!(Vec::<String>::new()),
                // extract ith column of release
                release_info: value_to_json(&get_ith_release(
                    release.array()?.i64()?,
                    &(column_number as usize)
                )?.into())?,
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number as usize].clone()),
                accuracy: None,
                batch: component.batch as u64,
                node_id: node_id.clone() as u64,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: self.implementation.clone(),
                    argument: serde_json::json!({}),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
