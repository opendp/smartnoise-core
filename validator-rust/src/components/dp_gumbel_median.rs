use crate::errors::*;

use crate::{proto, base, Warnable};
use crate::components::{Report, Component};

use crate::base::{NodeProperties, Value, ValueProperties, DataType, IndexKey, Array};
use crate::utilities::json::{JSONRelease, value_to_json, privacy_usage_to_json, AlgorithmInfo};
use crate::utilities::{prepend, privacy::spread_privacy_usage, array::get_ith_column};
use indexmap::map::IndexMap;


impl Component for proto::DpGumbelMedian {
    fn propagate_property(&self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let data_property = properties.get(&IndexKey::from("data"))
            .ok_or_else(|| Error::from("data: missing"))?.clone().array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }



        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        Ok(ValueProperties::Array(data_property).into())
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
                    Array::Int(v) => value_to_json(&get_ith_column(v, column_number)?.into())?,
                    _ => return Err("maximum must be numeric".into())
                },
                privacy_loss: privacy_usage_to_json(&privacy_usages[column_number].clone()),
                accuracy: None,
                submission: component.submission,
                node_id,
                postprocess: false,
                algorithm_info: AlgorithmInfo {
                    name: "".to_string(),
                    cite: "".to_string(),
                    mechanism: "exponential".into(),
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
