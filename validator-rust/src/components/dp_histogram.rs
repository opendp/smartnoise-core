use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable, Report, get_ith_release};


use crate::base::{NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, AlgorithmInfo, privacy_usage_to_json, value_to_json};
use crate::utilities::{prepend, broadcast_privacy_usage};


impl Component for proto::DpHistogram {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPCount is abstract, and has no property propagation".into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::DpHistogram {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = maximum_id.clone();
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        let data_id = component.arguments.get("data")
            .ok_or::<Error>("data is a required argument to DPHistogram".into())?;
        let edges_id = component.arguments.get("edges")
            .ok_or::<Error>("edges is a required argument to DPHistogram".into())?;
        let null_id = component.arguments.get("null")
            .ok_or::<Error>("null is a required argument to DPHistogram".into())?;
        let inclusive_left_id = component.arguments.get("inclusive_left")
            .ok_or::<Error>("inclusive_left is a required argument to DPHistogram".into())?;
        let count_min_id = component.arguments.get("count_min")
            .ok_or::<Error>("count_min is a required argument to DPHistogram".into())?;
        let count_max_id = component.arguments.get("count_max")
            .ok_or::<Error>("count_max is a required argument to DPHistogram".into())?;
        // TODO: also handle categorical case, which doesn't require binning
        // bin
        current_id += 1;
        let id_bin = current_id.clone();
        computation_graph.insert(id_bin, proto::Component {
            arguments: hashmap![
                "data".to_owned() => *data_id,
                "edges".to_owned() => *edges_id,
                "null".to_owned() => *null_id,
                "inclusive_left".to_owned() => *inclusive_left_id
            ],
            variant: Some(proto::component::Variant::from(proto::Bin {
                side: self.side.clone()
            })),
            omit: true,
            batch: component.batch,
        });

        // dp_count
        computation_graph.insert(component_id.clone(), proto::Component {
            arguments: hashmap![
                "data".to_owned() => id_bin,
                "count_min".to_owned() => *count_min_id,
                "count_max".to_owned() => *count_max_id
            ],
            variant: Some(proto::component::Variant::from(proto::DpCount {
                privacy_usage: self.privacy_usage.clone(),
                implementation: self.implementation.clone(),
            })),
            omit: false,
            batch: component.batch,
        });

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases: HashMap::new(),
            traversal: vec![id_bin]
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
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.get_min_f64()?;
        let maximums = data_property.get_max_f64()?;
        let num_records = data_property.get_num_records()?;

        let num_columns = data_property.get_num_columns()?;
        let privacy_usages = broadcast_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..num_columns {
            let release = JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPHistogram".to_string(),
                variables: serde_json::json!(Vec::<String>::new()),
                // extract ith column of release
                release_info: value_to_json(&get_ith_release(
                    release.get_arraynd()?.get_i64()?,
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
                    argument: serde_json::json!({
                            "n": num_records,
                            "constraint": {
                                "lowerbound": minimums[column_number as usize],
                                "upperbound": maximums[column_number as usize]
                            }
                        }),
                },
            };

            releases.push(release);
        }
        Ok(Some(releases))
    }
}
