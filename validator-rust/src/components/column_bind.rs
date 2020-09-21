use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component};
use crate::base::{Value, ValueProperties, ArrayProperties};
use crate::utilities::{get_common_value};
use indexmap::map::IndexMap;
use noisy_float::types::n64;
use num::ToPrimitive;


impl Component for proto::ColumnBind {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        let array_props: Vec<&ArrayProperties> = properties.values()
            .map(|v| v.array()).collect::<Result<_>>()?;

        let releasable = get_common_value(&array_props.iter().map(|v| v.releasable).collect())
            .ok_or_else(|| Error::from("arguments must all be releasable, or all be private"))?;

        let common_id = get_common_value(&array_props.iter().map(|v| v.dataset_id).collect());
        let dataset_id = if releasable {
            common_id.and_then(|v| v)
        } else {
            Some(common_id
                .ok_or_else(|| Error::from("private datasets must share the same dataset id"))?
                .ok_or_else(|| Error::from("dataset_id must be known for private datasets"))?)
        };

        if array_props.iter().any(|v| v.aggregator.is_some()) {
            return Err(Error::from("arguments must not be aggregated"))
        }

        Ok(Warnable::new(ValueProperties::Array(ArrayProperties {
            num_records: Some(get_common_value(&array_props.iter()
                .map(|v| v.num_records).collect::<Vec<_>>())
                .ok_or_else(|| Error::from("all record lengths must match"))?
                .ok_or_else(|| "num_records must be known when unioning")?),
            num_columns: array_props.iter()
                .try_fold(0, |sum, v| v.num_columns.map(|v| sum + v)),
            nullity: get_common_value(&array_props.iter().map(|v| v.nullity).collect())
                .unwrap_or(true),
            releasable,
            c_stability: get_common_value(&array_props.iter().map(|v| v.c_stability).collect())
                .ok_or_else(|| Error::from("column bind must share c-stability constants"))?,
            aggregator: None,
            // TODO: merge natures
            nature: None,
            data_type: get_common_value(&array_props.iter().map(|v| v.data_type.clone()).collect())
                .ok_or_else(|| "data_types must be equivalent when binding into homogeneous array")?,
            dataset_id,
            node_id: node_id as i64,
            is_not_empty: array_props.iter().any(|v| v.is_not_empty),
            dimensionality: Some(2),
            group_id: get_common_value(&array_props.iter().map(|v| v.group_id.clone()).collect())
                .ok_or_else(|| Error::from("group id must be shared among arguments"))?,
            naturally_ordered: get_common_value(&array_props.iter().map(|v| v.naturally_ordered).collect())
                .ok_or_else(|| Error::from("natural ordering must be shared among arguments"))?,
            sample_proportion: get_common_value(&array_props.iter().map(|v| v.sample_proportion.map(n64)).collect())
                .ok_or_else(|| Error::from("sample proportions must be shared among arguments"))?.and_then(|v| v.to_f64()),
        })))
    }
}