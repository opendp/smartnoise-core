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

        if data_property_x.data_type != DataType::Float {
            return Err("data_x: atomic type must be float".into());
        }

        if data_property_y.data_type != DataType::Float {
            return Err("data_y: atomic type must be float".into());
        }

        if data_property_x.num_records()? != data_property_y.num_records()? {
            return Err("data_x and data_y: must be same length".into());
        }

        let k = match self.implementation.to_lowercase().as_str() {
            "theil-sen" => data_property_x.num_records()? as u32 - 1,
            "theil-sen-match" => 1,
            "theil-sen-k-match" => self.k,
             _ => return Err("Invalid implementation passed. \
                     Valid values are theil-sen and theil-sen-k-match".into())
        };

        if !data_property_x.releasable && !data_property_y.releasable && data_property_x.group_id != data_property_y.group_id {
            return Err("data from separate partitions may not be mixed".into())
        }

        if data_property_x.dataset_id != data_property_y.dataset_id {
            return Err("left and right arguments must share the same dataset id".into())
        }
        // this check should be un-necessary due to the dataset id check
        if data_property_x.c_stability != data_property_y.c_stability {
            return Err(Error::from("left and right datasets must share the same stabilities"))
        }

        let output_properties = ArrayProperties {
            // TODO: these properties could be made tighter
            num_records: None,
            num_columns: Some(1),
            nullity: data_property_x.nullity || data_property_y.nullity,
            releasable: data_property_x.releasable && data_property_y.releasable,
            c_stability: data_property_x.c_stability * k,
            aggregator: None,
            nature: None,
            data_type: DataType::Float,
            dataset_id: Some(node_id as i64),
            node_id: node_id as i64,
            is_not_empty: false,
            dimensionality: Some(1),
            group_id: propagate_binary_group_id(&data_property_x, &data_property_y)?,
            naturally_ordered: false,
            sample_proportion: None
        };

        Ok(ValueProperties::Dataframe(DataframeProperties {
            children: indexmap![
                IndexKey::from("slope") => output_properties.clone().into(),
                IndexKey::from("intercept") => output_properties.into()]
        }).into())
    }
}
