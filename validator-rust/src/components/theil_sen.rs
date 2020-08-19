use crate::errors::*;

use crate::{proto, base, Warnable, Float};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, IndexKey};
use crate::utilities::prepend;
use ndarray::prelude::*;
use indexmap::map::IndexMap;
use std::ptr::null;


impl Component for proto::TheilSen {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let output_properties = ArrayProperties {
            num_records,
            num_columns: Some(1),
            nullity: false,
            releasable: x.releasable && y.releasable,
            c_stability: x.c_stability.iter().zip(y.c_stability.iter()).map(|(l, r)| l * r).collect(),
            aggregator: None,
            nature: None,
            data_type: DataType::Float,
            dataset_id: None,
            is_not_empty: true,
            dimensionality: Some(1),
            // TODO
            group_id: propagate_binary_group_id(&x, &y)?,
        };

        let mut data_property_x = properties.get::<IndexKey>(&"data_x".into())
            .ok_or("data x: missing")?.array()?
            .map_err(prepend("data x:"))?.clone();

        let mut data_property_y = properties.get::<IndexKey>(&"data_y".into())
            .ok_or("data y: missing")?.array()?
            .map_err(prepend("data y:"))?.clone();

        if !data_property_x.releasable {
            data_property_x.assert_is_not_aggregated()?;
        }
        if !data_property_y.releasable {
            data_property_y.assert_is_not_aggregated()?;
        }
        data_property_x.assert_is_not_empty()?;
        data_property_y.assert_is_not_empty()?;


        if data_property_x.data_type != DataType::Float {
            return Err("data x: atomic type must be float".into());
        }

        if data_property_y.data_type != DataType::Float {
            return Err("data y: atomic type must be float".into());
        }

        if data_property_x.len() != data_property_y.len() {
            return Err("data x and data y: must be same length".into());
        }

        let num_records = match self.implementation.to_lowercase().as_str() {
            "theil-sen" => data_property_x.powi(2),
            "theil-sen-k-match" => self.k * (data_property_x / 2.0).floor(),
            _ => return Err("Invalid implementation passed. \
                Valid values are theil-sen and theil-sen-k-match".into())
        };
        output_properties.num_records = Some(num_records);
        output_properties.dataset_id = Some(node_id as i64);

        Ok(ValueProperties::Dataframe(DataframeProperties {
            children: indexmap!["slope" => output_properties.into(),
                                "intercept" => output_properties.into()]
        }).into())
    }
}
