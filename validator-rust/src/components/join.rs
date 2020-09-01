use crate::errors::*;

use crate::{Warnable, base, Float};
use indexmap::map::IndexMap;
use crate::base::{Value, ValueProperties, IndexKey, ArrayProperties, DataframeProperties};
use crate::components::Component;

impl Component for proto::Join {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let mut left_property = properties.remove::<IndexKey>(&"left")
            .ok_or("left: missing")?.clone();
        let mut right_property = properties.remove::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.clone();

        let left_on = public_arguments.remove::<IndexKey>(&"left_on".into())
            .ok_or_else(|| Error::from("left_on: missing, must be public"))
            .clone().array()?;
        let right_on = public_arguments.remove::<IndexKey>(&"right_on".into())
            .ok_or_else(|| Error::from("right_on: missing, must be public"))
            .clone().array()?;

        match (left_property, right_property) {
            (
                ValueProperties::Array(mut left_property),
                ValueProperties::Array(mut right_property)
            ) => {
                let left_index = left_on.first_int()?;
                let right_index = right_on.first_int()?;

                let left_num_columns = left_property.num_columns()?;
                if left_num_columns <= left_index as i64 {
                    return Err("left column index is out of bounds".into());
                }

                let right_num_columns = right_property.num_columns()?;
                if right_num_columns <= right_index as i64 {
                    return Err("right column index is out of bounds".into());
                }

                let left_c_stability = left_property.c_stability
                    .drain(left_index..left_index + 1).next();
                let right_c_stability = right_property.c_stability
                    .drain(right_index..right_index + 1).next();

                left_property.assert_is_not_aggregated()?;
                right_property.assert_is_not_aggregated()?;

                if left_property.data_type != right_property.data_type {
                    return Err("data types must be homogenous when joining arrays".into())
                }

                if !left_property.group_id.is_empty() || !right_property.group_id.is_empty() {
                    return Err("joined arrays must not be partitioned".into())
                }

                Ok(ValueProperties::Array(ArrayProperties {
                    num_records: None,
                    num_columns: Some(left_num_columns + right_num_columns - 1),
                    nullity: left_property.nullity || right_property.nullity,
                    releasable: left_property.releasable && right_property.releasable,
                    c_stability: vec![left_c_stability * right_c_stability].into_iter()
                        .chain(left_property.c_stability)
                        .chain(right_property.c_stability)
                        .map(|v| v * self.k)
                        .collect(),
                    aggregator: None,
                    // TODO: preserve natures through join
                    nature: None,
                    data_type: left_property.data_type,
                    dataset_id: Some(node_id as i64),
                    is_not_empty: false,
                    dimensionality: Some(2),
                    group_id: vec![],
                }).into())
            }
            (
                ValueProperties::Dataframe(mut left_property),
                ValueProperties::Dataframe(mut right_property)
            ) => {
                let left_join_prop = left_property.children
                    .remove(IndexKey::new(left_on)?)
                    .ok_or_else(|| "left column name does not exist")?
                    .array()?.clone();

                let right_join_prop = right_property.children
                    .remove(IndexKey::new(right_on)?)
                    .ok_or_else(|| "right column name does not exist")?
                    .array()?.clone();

                let left_c_stability = left_join_prop.c_stability[0];
                let right_c_stability = right_join_prop.c_stability[0];

                let join_property = ArrayProperties {
                    num_records: None,
                    num_columns: Some(1),
                    nullity: left_join_prop.nullity || right_join_prop.nullity,
                    releasable: left_property.releasable && right_property.releasable,
                    c_stability: vec![left_c_stability * right_c_stability].into_iter()
                        .chain(left_property.c_stability)
                        .chain(right_property.c_stability)
                        .map(|v| v * self.k)
                        .collect(),
                    aggregator: None,
                    // TODO: preserve natures through join
                    nature: None,
                    data_type: left_property.data_type,
                    dataset_id: Some(node_id as i64),
                    is_not_empty: false,
                    dimensionality: Some(2),
                    group_id: vec![],
                };

                left_property.assert_is_not_aggregated()?;
                right_property.assert_is_not_aggregated()?;

                if left_property.data_type != right_property.data_type {
                    return Err("data types must be homogenous when joining arrays".into())
                }

                if !left_property.group_id.is_empty() || !right_property.group_id.is_empty() {
                    return Err("joined arrays must not be partitioned".into())
                }

                let c_stability = vec![left_join_prop.c_stability[0] * right_join_prop.c_stability[0]].into_iter()
                    .chain(left_property.children.values()
                        .map(|v| Ok(v.array()?.c_stability[0]))
                        .collect::<Result<Vec<Float>>>()?)
                    .chain(right_property.children.values()
                        .map(|v| Ok(v.array()?.c_stability[0]))
                        .collect::<Result<Vec<Float>>>()?)
                    .map(|v| v * self.k)
                    .collect::<Vec<Float>>();

                

                Ok(ValueProperties::Dataframe(DataframeProperties {
                    children: c_stability.into_iter().map(|v| ArrayProperties {
                        num_records: None,
                        num_columns: Some(left_num_columns + right_num_columns - 1),
                        nullity: left_property.nullity || right_property.nullity,
                        releasable: left_property.releasable && right_property.releasable,
                        c_stability: vec![left_c_stability * right_c_stability].into_iter()
                            .chain(left_property.c_stability)
                            .chain(right_property.c_stability)
                            .map(|v| v * self.k)
                            .collect(),
                        aggregator: None,
                        // TODO: preserve natures through join
                        nature: None,
                        data_type: left_property.data_type,
                        dataset_id: Some(node_id as i64),
                        is_not_empty: false,
                        dimensionality: Some(2),
                        group_id: vec![],
                    }.into()).collect::<IndexMap<IndexKey>>
                }))
            }
            _ => return Err("left and right arguments must be both arrays or dataframes".into())
        }
    }
}
