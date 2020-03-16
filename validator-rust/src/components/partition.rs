use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, Vector2DJagged, NodeProperties, AggregatorProperties, SensitivityType, prepend, ValueProperties, HashmapProperties, ArrayNDProperties};


impl Component for proto::Partition {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        Ok(match properties.get("by") {
            Some(by_property) => {
                let by_property = by_property.get_arraynd()
                    .map_err(prepend("by:"))?.clone();
                let by_num_columns= by_property.num_columns
                    .ok_or::<Error>("number of columns must be known on by".into())?;
                if by_num_columns != 1 {
                    return Err("Partition's by argument must contain a single column".into());
                }
                let categories = by_property.get_categories()
                    .map_err(prepend("by:"))?;
                data_property.num_records = None;

                HashmapProperties {
                    num_records: data_property.num_records,
                    disjoint: true,
                    columnar: false,
                    properties: match categories {
                        Vector2DJagged::Bool(categories) => broadcast_partitions(&categories, &data_property)?.into(),
                        Vector2DJagged::Str(categories) => broadcast_partitions(&categories, &data_property)?.into(),
                        Vector2DJagged::I64(categories) => broadcast_partitions(&categories, &data_property)?.into(),
                        _ => return Err("partitioning based on floats is not supported".into())
                    }
                }
            },
            None => {

                let num_partitions = public_arguments.get("num_partitions")
                    .ok_or("num_partitions or by must be passed to Partition")?.get_arraynd()?.get_first_i64()?;

                let lengths = match data_property.num_records {
                    Some(num_records) => (0..num_partitions)
                        .map(|index| Some(num_records / num_partitions + (if index > (num_records % num_partitions) {0} else {1})))
                        .collect::<Vec<Option<i64>>>(),
                    None => (0..num_partitions)
                        .map(|_| None)
                        .collect::<Vec<Option<i64>>>()
                };

                HashmapProperties {
                    num_records: data_property.num_records,
                    disjoint: true,
                    columnar: false,
                    properties: lengths.iter().enumerate().map(|(index, partition_num_records)| {
                        let mut partition_property = data_property.clone();
                        partition_property.num_records = partition_num_records.clone();
                        (index as i64, ValueProperties::ArrayND(partition_property))
                    }).collect::<HashMap<i64, ValueProperties>>().into()
                }
            }
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

pub fn broadcast_partitions<T: Clone + Eq + std::hash::Hash>(
    categories: &Vec<Option<Vec<T>>>, properties: &ArrayNDProperties
) -> Result<HashMap<T, ValueProperties>> {

    if categories.len() != 1 {
        return Err("categories: must be defined for one column".into())
    }
    let partitions = categories[0].clone()
        .ok_or::<Error>("categories: must be defined".into())?;
    Ok(partitions.iter()
        .map(|v| (v.clone(), ValueProperties::ArrayND(properties.clone())))
        .collect())
}