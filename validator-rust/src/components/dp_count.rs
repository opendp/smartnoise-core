use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable, Report};


use crate::base::{NodeProperties, Value, ValueProperties};
use crate::utilities::json::{JSONRelease, privacy_usage_to_json, AlgorithmInfo, value_to_json};


impl Component for proto::DpCount {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Err("DPCount is abstract, and has no property propagation".into())
    }


}


impl Expandable for proto::DpCount {
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        _properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut maximum_id = maximum_id.clone();
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();

        // count
        maximum_id += 1;
        let id_count = maximum_id.clone();
        computation_graph.insert(id_count.clone(), proto::Component {
            arguments: hashmap!["data".to_owned() => *component.arguments.get("data").ok_or::<Error>("data must be provided as an argument".into())?],
            variant: Some(proto::component::Variant::Count(proto::Count {})),
            omit: true,
            batch: component.batch,
        });

        // noising
        computation_graph.insert(component_id.clone(), proto::Component {
            arguments: hashmap![
                "data".to_owned() => id_count,
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
            traversal: vec![id_count]
        })
    }
}

impl Report for proto::DpCount {
    fn summarize(
        &self,
        node_id: &u32,
        component: &proto::Component,
        _public_arguments: &HashMap<String, Value>,
        _properties: &NodeProperties,
        release: &Value
    ) -> Result<Option<Vec<JSONRelease>>> {
        Ok(Some(vec![JSONRelease {
            description: "DP release information".to_string(),
            statistic: "DPCount".to_string(),
            variables: serde_json::json!(Vec::<String>::new()),
            release_info: value_to_json(&release)?,
            privacy_loss: privacy_usage_to_json(&self.privacy_usage[0].clone()),
            accuracy: None,
            batch: component.batch as u64,
            node_id: node_id.clone() as u64,
            postprocess: false,
            algorithm_info: AlgorithmInfo {
                name: "".to_string(),
                cite: "".to_string(),
                mechanism: self.implementation.clone(),
                argument: serde_json::json!({})
            }
        }]))
    }
}
