use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component, Expandable};
use crate::base::{IndexKey, Value, Jagged, ValueProperties, IndexmapProperties, ArrayProperties, NodeProperties};
use crate::utilities::{prepend, get_literal, get_argument};
use indexmap::map::IndexMap;
use itertools::Itertools;
use crate::utilities::inference::infer_property;


impl Component for proto::Partition {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &IndexMap<base::IndexKey, &Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.clone();

        Ok(ValueProperties::Indexmap(match properties.get::<IndexKey>(&"by".into()) {

            // propagate properties when partitioning "by" some array
            Some(by_property) => {
                // TODO: pass non-homogeneously typed keys via indexmap (needs merge component)
                let by_property = by_property.array()
                    .map_err(prepend("by:"))?.clone();
                by_property.num_columns
                    .ok_or_else(|| Error::from("number of columns must be known on by"))?;
                let categories = by_property.categories()
                    .map_err(prepend("by:"))?;

                let partition_keys = make_dense_partition_keys(&categories, by_property.dimensionality)?;

                IndexmapProperties {
                    children: broadcast_partitions(partition_keys, &data_property, node_id)?,
                    variant: proto::indexmap_properties::Variant::Partition,
                }
            }

            // propagate properties when partitioning evenly
            None => {
                let num_partitions = get_argument(public_arguments, "num_partitions")?
                    .array()?.first_i64()?;

                let num_records = match &data_property {
                    ValueProperties::Array(data_property) => data_property.num_records,
                    ValueProperties::Indexmap(data_property) => data_property.num_records()?,
                    _ => return Err("data: must be a dataframe or array".into())
                };
                let lengths = match num_records {
                    Some(num_records) => even_split_lengths(num_records, num_partitions)
                        .into_iter().map(Some).collect(),
                    None => (0..num_partitions)
                        .map(|_| None)
                        .collect::<Vec<Option<i64>>>()
                };

                IndexmapProperties {
                    children: lengths.iter().enumerate()
                        .map(|(index, partition_num_records)| Ok((
                            IndexKey::from(index as i64),
                            get_partition_properties(&data_property, partition_num_records.clone(), node_id)?
                        )))
                        .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
                    variant: proto::indexmap_properties::Variant::Partition,
                }
            }
        }).into())
    }
}

impl Expandable for proto::Partition {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: &u32,
        maximum_id: &u32
    ) -> Result<base::ComponentExpansion> {

        let mut current_id = *maximum_id;

        let mut expansion = base::ComponentExpansion::default();

        if let Some(by) = properties.get::<IndexKey>(&"by".into()) {
            if !properties.contains_key::<IndexKey>(&"categories".into()) {
                let categories = by.array()?.categories()?;
                current_id += 1;
                let id_categories = current_id;
                let (patch_node, release) = get_literal(Value::Jagged(categories), component.submission)?;
                expansion.computation_graph.insert(id_categories, patch_node);
                expansion.properties.insert(id_categories, infer_property(&release.value, None)?);
                expansion.releases.insert(id_categories, release);

                let mut component = component.clone();
                component.insert_argument(&"categories".into(), id_categories);
                expansion.computation_graph.insert(*component_id, component);
            }
        }

        Ok(expansion)
    }
}

pub fn broadcast_partitions(
    partition_keys: Vec<IndexKey>, properties: &ValueProperties, node_id: u32
) -> Result<IndexMap<IndexKey, ValueProperties>> {
    // create dense partitioning
    partition_keys.into_iter()
        .map(|v| Ok((v, get_partition_properties(properties, None, node_id)?)))
        .collect()
}

fn get_partition_properties(
    properties: &ValueProperties, num_records: Option<i64>, node_id: u32
) -> Result<ValueProperties> {

    let update_array_properties = |mut properties: ArrayProperties| -> ArrayProperties {
        // update properties
        properties.group_id.push(base::GroupId {
            partition_id: node_id,
            index: None
        });
        properties.num_records = num_records;
        properties.dataset_id = Some(node_id as i64);
        properties.is_not_empty = num_records.unwrap_or(0) != 0;

        properties
    };

    Ok(match properties {
        ValueProperties::Array(properties) => ValueProperties::Array(update_array_properties(properties.clone())),
        ValueProperties::Indexmap(properties) => {
            if properties.variant != proto::indexmap_properties::Variant::Dataframe {
                return Err("data: indexmap must be dataframe".into())
            }
            let mut properties = properties.clone();
            properties.children.values_mut()
                .map(|v| {
                    *v = ValueProperties::Array(update_array_properties(v.array()?.clone()));
                    Ok(())
                })
                .collect::<Result<()>>()?;
            ValueProperties::Indexmap(properties)
        }
        _ => return Err("data: must be a dataframe or array".into())
    })
}

pub fn make_dense_partition_keys(categories: &Jagged, dimensionality: Option<i64>) -> Result<Vec<IndexKey>> {
    let categories = categories.to_index_keys()?;

    // TODO: sparse partitioning component
    Ok(match dimensionality {
        Some(0) => return Err("categories: must be defined for at least one column".into()),
        Some(1) => {
            if categories.len() != 1 {
                return Err("categories: must be defined for exactly one column".into())
            }
            categories[0].clone()
        }
        _ => categories.clone().into_iter().multi_cartesian_product()
            .map(IndexKey::Tuple).collect()
    })
}

pub fn even_split_lengths(num_records: i64, num_partitions: i64) -> Vec<i64> {
    (0..num_partitions)
        .map(|index| num_records / num_partitions + (if index >= (num_records % num_partitions) { 0 } else { 1 }))
        .collect()
}


#[cfg(test)]
mod test_partition {
    use crate::components::partition::even_split_lengths;

    fn vec_eq(left: &Vec<i64>, right: &Vec<i64>) -> bool {
        (left.len() == right.len()) && left.iter().zip(right)
            .all(|(a, b)| a == b)
    }

    #[test]
    fn test_units() {
        assert!(vec_eq(
            &even_split_lengths(4, 3),
            &vec![2, 1, 1]));
        assert!(vec_eq(
            &even_split_lengths(5, 3),
            &vec![2, 2, 1]));
        assert!(vec_eq(
            &even_split_lengths(3, 3),
            &vec![1, 1, 1]));
        assert!(vec_eq(
            &even_split_lengths(2, 3),
            &vec![1, 1, 0]));
        assert!(vec_eq(
            &even_split_lengths(2, 0),
            &vec![]));
    }
}