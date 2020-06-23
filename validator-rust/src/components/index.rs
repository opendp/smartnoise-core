use crate::errors::*;

use crate::base::{
    Array, Value, ValueProperties, ArrayProperties,
    IndexKey, NodeProperties
};

use crate::{proto, base, Warnable};
use crate::components::{Component, Named, Expandable};

use ndarray::ArrayD;
use ndarray::prelude::*;
use crate::utilities::{get_literal, get_argument};
use indexmap::map::IndexMap;
use crate::utilities::properties::{select_properties, stack_properties};
use crate::utilities::inference::infer_property;

impl Component for proto::Index {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &IndexMap<base::IndexKey, &Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.clone();

        let mut dimensionality = None;

        let properties = match data_property {
            ValueProperties::Indexmap(data_property) => {

                match data_property.variant {


                    proto::indexmap_properties::Variant::Dataframe => {
                        if let Some(column_names) = public_arguments.get::<IndexKey>(&"names".into()) {
                            let column_names = column_names.array()?;
                            dimensionality = Some(column_names.shape().len() as i64 + 1);
                            match column_names.to_owned() {
                                Array::Float(_) => return Err("floats are not valid indexes".into()),
                                Array::Int(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Str(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Bool(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                            }
                                .ok_or_else(|| Error::from("columns: unknown column in index"))

                        } else if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                            let indices = indices.array()?.int()?;
                            dimensionality = Some(indices.shape().len() as i64 + 1);
                            to_name_vec(indices)?.into_iter()
                                .map(|idx| data_property.children.get_index(idx as usize).map(|v| v.1.clone()))
                                .collect::<Option<Vec<ValueProperties>>>()
                                .ok_or_else(|| Error::from("index does not exist"))

                        } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                            let mask = mask.array()?.bool()?;
                            if mask.shape().len() != 1 {
                                return Err("mask: must be 1-dimensional".into())
                            }
                            dimensionality = Some(2);
                            let mask = to_name_vec(mask)?;
                            if mask.len() != data_property.children.len() {
                                return Err("mask: must be same length as the number of columns".into())
                            }
                            Ok(data_property.children.into_iter().zip(mask)
                                .filter(|(_, m)| *m).map(|(v, _)| v.1)
                                .collect::<Vec<ValueProperties>>())
                        } else {
                            return Err("one of names, indices or mask must be supplied".into())
                        }
                    }

                    // index into a partitional indexmap
                    proto::indexmap_properties::Variant::Partition => {
                        let names = get_argument(public_arguments, "names")?
                            .to_owned().array()?.clone();

                        let partition_key = IndexKey::new(names)?;
                        let mut part_properties = data_property.children.get::<IndexKey>(&partition_key)
                            .ok_or_else(|| format!("unknown partition index: {:?}", partition_key))?.clone();

                        fn set_group_index(part_properties: &mut ArrayProperties, key: IndexKey) {
                            let last_idx = part_properties.group_id.len() - 1;

                            if let Some(v) = part_properties.group_id.get_mut(last_idx) {
                                v.index = Some(key)
                            };
                        }

                        match &mut part_properties {
                            ValueProperties::Array(part_properties) =>
                                set_group_index(part_properties, partition_key),
                            ValueProperties::Indexmap(part_properties) =>
                                part_properties.children.values_mut()
                                    .try_for_each(|mut v| if let ValueProperties::Array(v) = &mut v {
                                        set_group_index(v, partition_key.clone());
                                        Ok(())
                                    } else { Err(Error::from("dataframe columns must be arrays")) })?,
                            _ => return Err("data: partition members must be either a dataframe or array".into())
                        }

                        return Ok(Warnable::new(part_properties))
                    }
                }
            },


            ValueProperties::Array(data_property) => {
                if !data_property.releasable {
                    data_property.assert_is_not_aggregated()?;
                }

                if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                    let indices = indices.array()?.int()?;
                    dimensionality = Some(indices.shape().len() as i64 + 1);

                    to_name_vec(indices)?.into_iter()
                        .map(|idx| select_properties(&data_property, idx as usize))
                        .collect::<Result<Vec<ValueProperties>>>()

                } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                    let mask = mask.array()?.bool()?;
                    if mask.shape().len() != 1 {
                        return Err("mask: must be 1-dimensional".into())
                    }
                    dimensionality = Some(2);
                    let mask = to_name_vec(mask)?;
                    if mask.len() != data_property.num_columns()? as usize {
                        return Err("mask: must be same length as the number of columns".into())
                    }
                    mask.into_iter().enumerate().filter(|(_, mask)| *mask)
                        .map(|(idx, _)| select_properties(&data_property, idx))
                        .collect::<Result<Vec<ValueProperties>>>()
                } else {
                    return Err("either indices or mask must be supplied".into())
                }
            },
            ValueProperties::Jagged(_) => Err("indexing is not supported on vectors".into()),
            ValueProperties::Function(_) => Err("indexing is not suppported on functions".into())
        }?;

        stack_properties(&properties, dimensionality).map(Warnable::new)
    }
}

impl Expandable for proto::Index {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: u32,
        mut maximum_id: u32
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();

        let data_property: ValueProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.clone();

        if let Ok(indexmap) = data_property.indexmap() {
            if indexmap.variant == proto::indexmap_properties::Variant::Partition {
                maximum_id += 1;
                let id_is_partition = maximum_id;
                let (patch_node, release) = get_literal(true.into(), component.submission)?;
                expansion.computation_graph.insert(id_is_partition, patch_node);
                expansion.properties.insert(id_is_partition, infer_property(&release.value, None)?);
                expansion.releases.insert(id_is_partition, release);

                let mut component = component.clone();
                component.insert_argument(&"is_partition".into(), id_is_partition);
                expansion.computation_graph.insert(component_id, component);
            }
        }

        Ok(expansion)
    }
}

impl Named for proto::Index {
    fn get_names(
        &self,
        public_arguments: &IndexMap<base::IndexKey, &Value>,
        argument_variables: &IndexMap<base::IndexKey, Vec<IndexKey>>,
        _release: Option<&Value>
    ) -> Result<Vec<IndexKey>> {
        if let Some(names) = public_arguments.get::<IndexKey>(&"names".into()) {
            return Ok(match names.array()? {
                Array::Int(names) => names.iter()
                    .map(|n| n.clone().into())
                    .collect(),
                Array::Bool(names) => names.iter()
                    .map(|n| n.clone().into())
                    .collect(),
                Array::Str(names) => names.iter()
                    .map(|n| n.clone().into())
                    .collect(),
                _ => return Err("column names may not be floats".into())
            })
        }
        let input_names = argument_variables.get::<IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("column names on data must be known"))?;

        if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
            indices.array()?.int()?.iter()
                .map(|idx| input_names.get(*idx as usize).cloned())
                .collect::<Option<Vec<IndexKey>>>()
                .ok_or_else(|| Error::from("attempted to retrieve an out-of-bounds name"))
        } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
            Ok(mask.array()?.bool()?.iter()
                .zip(input_names.iter())
                .filter(|(&mask, _)| mask)
                .map(|(_, name)| name.clone())
                .collect::<Vec<IndexKey>>())
        } else {
            Err("one of names, indices or mask must be supplied".into())
        }
    }
}

pub fn to_name_vec<T: Clone>(columns: &ArrayD<T>) -> Result<Vec<T>> {
    match columns.ndim() {
        0 => Ok(vec![columns.first()
            .ok_or_else(|| Error::from("At least one column name must be supplied"))?.clone()]),
        1 => match columns.clone().into_dimensionality::<Ix1>() {
            Ok(columns) => Ok(columns.to_vec()),
            Err(_) => Err("column names must be 1-dimensional".into())
        },
        _ => Err("dimensionality of column names must be less than 2".into())
    }
}
