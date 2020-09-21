use indexmap::map::IndexMap;

use crate::{base, proto, Warnable};
use crate::base::{Array, ArrayProperties, DataType, IndexKey, NodeProperties, Value, ValueProperties};
use crate::components::{Component, Expandable, Report};
use crate::errors::*;
use crate::utilities::{array::get_ith_column, prepend, privacy::spread_privacy_usage};
use crate::utilities::json::{AlgorithmInfo, JSONRelease, privacy_usage_to_json, value_to_json};

impl Component for proto::DpGumbelMedian {
    fn propagate_property(&self,
                          _privacy_definition: &Option<proto::PrivacyDefinition>,
                          _public_arguments: IndexMap<base::IndexKey, &Value>,
                          properties: NodeProperties,
                          node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let data_property: ArrayProperties = properties.get(&IndexKey::from("data"))
            .ok_or_else(|| Error::from("data: missing"))?.clone().array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        if data_property.num_columns()? != 1 {
            return Err(Error::from("dp gumbel median only works with one column at a time"))
        }

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        Ok(ValueProperties::Array(ArrayProperties {
            num_records: Some(1),
            num_columns: Some(1),
            nullity: false,
            releasable: true,
            c_stability: 1,
            aggregator: None,
            nature: None,
            data_type: data_property.data_type.clone(),
            dataset_id: None,
            node_id: node_id as i64,
            is_not_empty: true,
            dimensionality: Some(0),
            group_id: data_property.group_id,
            naturally_ordered: true,
            sample_proportion: None,
        }).into())
    }
}

impl Expandable for proto::DpGumbelMedian {
    fn expand_component(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        _maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let mut expansion = base::ComponentExpansion::default();

        let data_property: ArrayProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let privacy_definition = privacy_definition.as_ref()
            .ok_or_else(|| "privacy definition must be defined")?;

        if self.privacy_usage.len() != 1 {
            return Err(Error::from("privacy usage must be of length one"));
        }

        // update the privacy usage
        let mut updated_component = component.clone();
        if let Some(proto::component::Variant::DpGumbelMedian(variant)) = &mut updated_component.variant {
            variant.privacy_usage = vec![self.privacy_usage[0].actual_to_effective(
                data_property.sample_proportion.unwrap_or(1.),
                data_property.c_stability,
                privacy_definition.group_size)?];
            // this case should never happen
        } else { return Err(Error::from("Variant must be defined")) }
        expansion.computation_graph.insert(component_id, updated_component);

        Ok(expansion)
    }
}

impl Report for proto::DpGumbelMedian {
    fn summarize(
        &self,
        node_id: u32,
        component: &proto::Component,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        release: &Value,
        variable_names: Option<&Vec<base::IndexKey>>,
    ) -> Result<Option<Vec<JSONRelease>>> {
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut releases = Vec::new();

        let minimums = data_property.lower_float().unwrap();
        let maximums = data_property.upper_float().unwrap();

        let num_columns = data_property.num_columns()?;
        let privacy_usages = spread_privacy_usage(&self.privacy_usage, num_columns as usize)?;

        for column_number in 0..(num_columns as usize) {
            let variable_name = variable_names
                .and_then(|names| names.get(column_number)).cloned()
                .unwrap_or_else(|| "[Unknown]".into());

            releases.push(JSONRelease {
                description: "DP release information".to_string(),
                statistic: "DPGumbelMedian".to_string(),
                variables: serde_json::json!(variable_name.to_string()),
                release_info: match release.ref_array()? {
                    Array::Float(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    _ => return Err("release must be float".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
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
                            "lowerbound": minimums[column_number],
                            "upperbound": maximums[column_number]
                        }
                    }),
                },
            });
        }
        Ok(Some(releases))
    }
}
