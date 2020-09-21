use indexmap::map::IndexMap;

use crate::{base, proto};
use crate::base::{IndexKey, NodeProperties, Value};
use crate::components::{Expandable, Report};
use crate::errors::*;
use crate::utilities::{get_literal};
use crate::utilities::inference::infer_property;
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};
use crate::utilities::privacy::spread_privacy_usage;

impl Expandable for proto::DpLinearRegression {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        public_arguments: &IndexMap<IndexKey, &Value>,
        _properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        const DEFAULT_K: u32 = 100;

        let mut privacy_usages = spread_privacy_usage(&self.privacy_usage, 2)?;
        let slope_privacy_usage = privacy_usages.remove(0);
        let intercept_privacy_usage = privacy_usages.remove(0);

        let mut expansion = base::ComponentExpansion::default();
        let id_data_x = *component.arguments().get::<base::IndexKey>(&"data_x".into())
            .ok_or_else(|| Error::from("data must be provided as an argument"))?;
        let id_data_y = *component.arguments().get::<base::IndexKey>(&"data_y".into())
            .ok_or_else(|| Error::from("data must be provided as an argument"))?;
        let mut arguments = indexmap!["data_x".into() => id_data_x, "data_y".into() => id_data_y];

        match self.implementation.to_lowercase().as_str() {
            "theil-sen" => (),
            "theil-sen-k-match" => {

                arguments.insert("k".into(), if let Some(id_k) = component.arguments().get::<base::IndexKey>(&"k".into()) {
                    *id_k
                } else {
                    maximum_id += 1;
                    let id_k = maximum_id.to_owned();
                    let value = Value::from(DEFAULT_K as i64);
                    expansion.properties.insert(id_k, infer_property(&value, None, id_k)?);
                    let (patch_node, release) = get_literal(value, component.submission)?;
                    expansion.computation_graph.insert(id_k, patch_node);
                    expansion.releases.insert(id_k, release);
                    id_k
                });
            }
            _ => return Err(Error::from("Invalid implementation argument"))
        }

        // theil-sen transform
        maximum_id += 1;
        let id_theil_sen = maximum_id;
        expansion.computation_graph.insert(id_theil_sen, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(arguments)),
            variant: Some(proto::component::Variant::TheilSen(proto::TheilSen {
                implementation: self.implementation.clone(),
                k: if let Some(k) = public_arguments.get(&IndexKey::from("k")) {
                    k.ref_array()?.first_int()? as u32
                } else { DEFAULT_K },
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_theil_sen);

        // slope name
        maximum_id += 1;
        let id_slope_name = maximum_id;
        let value = Value::from("slope".to_string());
        expansion.properties.insert(id_slope_name, infer_property(&value, None, id_slope_name)?);
        let (patch_node, release) = get_literal(value, component.submission)?;
        expansion.computation_graph.insert(id_slope_name, patch_node);
        expansion.releases.insert(id_slope_name, release);

        // slope index
        maximum_id += 1;
        let id_slope_index = maximum_id;
        expansion.computation_graph.insert(id_slope_index, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_theil_sen, "names".into() => id_slope_name])),
            variant: Some(proto::component::Variant::Index(proto::Index {})),
            omit: true,
            submission: component.submission
        });
        expansion.traversal.push(id_slope_index);

        // slope dp median
        maximum_id += 1;
        let id_slope_dp_median = maximum_id;
        expansion.computation_graph.insert(id_slope_dp_median, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_slope_index])),
            variant: Some(proto::component::Variant::DpMedian(proto::DpMedian {
                mechanism: "gumbel".to_string(),
                privacy_usage: vec![slope_privacy_usage],
                interpolation: "midpoint".to_string(),
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_slope_dp_median);

        // intercept name
        maximum_id += 1;
        let id_intercept_name = maximum_id;
        let value = Value::from("intercept".to_string());
        expansion.properties.insert(id_intercept_name, infer_property(&value, None, id_intercept_name)?);
        let (patch_node, release) = get_literal(value, component.submission)?;
        expansion.computation_graph.insert(id_intercept_name, patch_node);
        expansion.releases.insert(id_intercept_name, release);

        // intercept index
        maximum_id += 1;
        let id_intercept_index = maximum_id;
        expansion.computation_graph.insert(id_intercept_index, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_theil_sen, "names".into() => id_intercept_name])),
            variant: Some(proto::component::Variant::Index(proto::Index {})),
            omit: true,
            submission: component.submission
        });
        expansion.traversal.push(id_intercept_index);

        // intercept dp median
        maximum_id += 1;
        let id_intercept_dp_median = maximum_id;
        expansion.computation_graph.insert(id_intercept_dp_median, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_intercept_index])),
            variant: Some(proto::component::Variant::DpMedian(proto::DpMedian {
                mechanism: "gumbel".to_string(),
                privacy_usage: vec![intercept_privacy_usage],
                interpolation: "midpoint".to_string(),
            })),
            omit: true,
            submission: component.submission,
        });
        expansion.traversal.push(id_intercept_dp_median);

        // bind together
        expansion.computation_graph.insert(component_id, proto::Component {
            arguments: Some(proto::ArgumentNodeIds::new(indexmap!["slope".into() => id_slope_index, "intercept".into() => id_intercept_index])),
            variant: Some(proto::component::Variant::ColumnBind(proto::ColumnBind {})),
            omit: component.omit,
            submission: component.submission,
        });
        expansion.traversal.push(component_id);

        Ok(expansion)
    }
}


impl Report for proto::DpLinearRegression {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        _properties: NodeProperties,
        release: &Value,
        _variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let privacy_usage = spread_privacy_usage(
            &self.privacy_usage, 2)?;

        let release = JSONRelease {
            description: "DP release information".to_string(),
            statistic: "DPLinearRegression".to_string(),
            variables: serde_json::json!(["slope", "intercept"]),
            release_info: value_to_json(release)?,
            privacy_loss: serde_json::json!(privacy_usage.iter().map(privacy_usage_to_json).collect::<Vec<_>>()),
            accuracy: None,
            submission: component.submission,
            node_id,
            postprocess: false,
            algorithm_info: AlgorithmInfo {
                name: "".to_string(),
                cite: "".to_string(),
                mechanism: "gumbel exponential".into(),
                argument: serde_json::json!({
                    "constraint": {
                    }
                }),
            },
        };
        Ok(Some(vec![release]))
    }
}
