use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component};
use crate::base::{Value, ValueProperties, DataType, IndexKey, ArrayProperties, DataframeProperties};
use crate::utilities::prepend;
use indexmap::map::IndexMap;
use crate::components::transforms::propagate_binary_group_id;


impl Component for proto::KMeansClustering {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property_x.assert_is_not_aggregated()?;
        }

        data_property.assert_is_not_empty()?;

        if data_property.data_type != DataType::Float {
            return Err("data: atomic type must be float".into());
        }

        let k = match self.implementation.to_lowercase().as_str() {
            "theil-sen" => data_property_x.num_records()? - 1,
            "theil-sen-match" => 1,
            "theil-sen-k-match" => self.k as i64,
            _ => return Err("Invalid implementation passed. \
                     Valid values are theil-sen and theil-sen-k-match".into())
        };

        let output_properties = ArrayProperties {
            // records may be null, then filtered
            num_records: None,
            num_columns: Some(1),
            nullity: data_property_x.nullity || data_property_y.nullity,
            releasable: data_property_x.releasable && data_property_y.releasable,
            c_stability: data_property_x.c_stability.iter().zip(data_property_y.c_stability.iter())
                .map(|(l, r)| l * r * k as f64).collect(),
            aggregator: None,
            nature: None,
            data_type: DataType::Float,
            dataset_id: Some(node_id as i64),
            node_id: node_id as i64,
            is_not_empty: true,
            dimensionality: Some(1),
            group_id: propagate_binary_group_id(&data_property_x, &data_property_y)?,
        };

        Ok(ValueProperties::Dataframe(DataframeProperties {
            children: indexmap![
                IndexKey::from("slope") => output_properties.clone().into(),
                IndexKey::from("intercept") => output_properties.into()]
        }).into())
    }
}
