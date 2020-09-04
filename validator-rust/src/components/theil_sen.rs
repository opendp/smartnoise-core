use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component};
use crate::base::{Value, ValueProperties, DataType, IndexKey, ArrayProperties, DataframeProperties};
use crate::utilities::prepend;
use indexmap::map::IndexMap;
use crate::components::transforms::propagate_binary_group_id;


impl Component for proto::TheilSen {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        let data_property_x = properties.get::<IndexKey>(&"data_x".into())
            .ok_or("data_x: missing")?.array()
            .map_err(prepend("data_x:"))?.clone();

        let data_property_y = properties.get::<IndexKey>(&"data_y".into())
            .ok_or("data_y: missing")?.array()
            .map_err(prepend("data_y:"))?.clone();

        if !data_property_x.releasable {
            data_property_x.assert_is_not_aggregated()?;
        }
        if !data_property_y.releasable {
            data_property_y.assert_is_not_aggregated()?;
        }
        data_property_x.assert_is_not_empty()?;
        data_property_y.assert_is_not_empty()?;

        data_property_x.assert_non_null()?;
        data_property_y.assert_non_null()?;

        if data_property_x.data_type != DataType::Float {
            return Err("data_x: atomic type must be float".into());
        }

        if data_property_y.data_type != DataType::Float {
            return Err("data_y: atomic type must be float".into());
        }

        if data_property_x.num_records != data_property_y.num_records {
            return Err("data_x and data_y: must be same length".into());
        }

        let num_records = data_property_x.num_records()?;

        let (k, num_records) = match self.implementation.to_lowercase().as_str() {
            "theil-sen" => (num_records / 2, (num_records / 2 * 2).pow(2)),
            "theil-sen-k-match" => (self.k as i64, ((self.k as i64) * num_records / 2) as i64),
             _ => return Err("Invalid implementation passed. \
                     Valid values are theil-sen and theil-sen-k-match".into())
        };

        let output_properties = ArrayProperties {
            num_records: Some(num_records),
            num_columns: Some(2),
            nullity: data_property_x.nullity || data_property_y.nullity,
            releasable: data_property_x.releasable && data_property_y.releasable,
            c_stability: data_property_x.c_stability.iter().zip(data_property_y.c_stability.iter()).map(|(l, r)| l * r * k as f64).collect(),
            aggregator: None,
            nature: None,
            data_type: DataType::Float,
            dataset_id: Some(node_id as i64),
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
