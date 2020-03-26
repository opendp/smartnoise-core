use crate::errors::*;

use std::collections::HashMap;
use crate::base::{ArrayND, Value, NodeProperties, ValueProperties, Hashmap, ArrayNDProperties, Nature, NatureContinuous, NatureCategorical, Vector1DNull, Vector2DJagged};

use crate::{proto, base};
use crate::components::Component;

use std::ops::Deref;
use ndarray::ArrayD;
use ndarray::prelude::*;

impl Component for proto::Index {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.clone();

        let column_names = public_arguments.get("columns")
            .ok_or::<Error>("columns: missing".into())?.deref().to_owned().get_arraynd()?.clone();

        let properties = match data_property {
            ValueProperties::Hashmap(data_property) => {
                // TODO: check that hashmap is columnar. The columnar property is in another branch.
                //       when partition is added, should we allow column stacking of partitions?
                match data_property.properties {
                    Hashmap::Str(value_properties) => match column_names {
                        // String column names on string hashmap
                        ArrayND::Str(column_names) => to_name_vec(&column_names)?.into_iter()
                            .map(|v| value_properties.get(&v).cloned())
                            .collect::<Option<Vec<ValueProperties>>>()
                            .ok_or::<Error>("columns: unknown column in index".into()),
                        // Bool mask on string hashmap
                        ArrayND::Bool(column_names) => {
                            let mask = to_name_vec(&column_names)?;
                            if value_properties.len() != mask.len() {
                                return Err("mask must be the same length as the number of columns".into());
                            }

                            Ok(mask.into_iter()
                                .zip(value_properties.values())
                                .filter(|(mask, _)| *mask)
                                .map(|(_, value)| value.clone())
                                .collect())
                        },
                        // Indices on string hashmap
                        ArrayND::I64(indices) => {
                            let indices = to_name_vec(&indices)?;
                            let column_names = value_properties.keys().cloned().collect::<Vec<String>>();
                            indices.iter().map(|index| value_properties.get(column_names.get(*index as usize)
                                .ok_or::<Error>("column index is out of range".into())?).cloned()
                                .ok_or::<Error>("properties not found".into())).collect::<Result<Vec<ValueProperties>>>()
                        },
                        ArrayND::F64(_) => Err("columns may not have float type".into())
                    },
                    Hashmap::I64(value_properties) => match column_names {
                        // I64 column names on I64 hashmap
                        ArrayND::I64(column_names) => to_name_vec(&column_names)?.into_iter()
                            .map(|v| value_properties.get(&v).cloned())
                            .collect::<Option<Vec<ValueProperties>>>()
                            .ok_or::<Error>("columns: unknown column in index".into()),
                        // Bool mask on I64 hashmap
                        ArrayND::Bool(column_names) => {
                            let mask = to_name_vec(&column_names)?;
                            if value_properties.len() != mask.len() {
                                return Err("mask must be the same length as the number of columns".into());
                            }

                            Ok(mask.into_iter()
                                .zip(value_properties.values())
                                .filter(|(mask, _)| *mask)
                                .map(|(_, value)| value.clone())
                                .collect())
                        },
                        _ => Err("columns must be either integer or a boolean mask".into())
                    },
                    Hashmap::Bool(value_properties) =>
                        to_name_vec(column_names.get_bool()?)?.into_iter()
                            .map(|name| value_properties.get(&name).cloned()
                                .ok_or::<Error>("columns: unknown column in index".into()))
                            .collect::<Result<Vec<ValueProperties>>>()
                }
            },
            ValueProperties::ArrayND(data_property) => match column_names {
                ArrayND::I64(indices) => to_name_vec(&indices)?.into_iter()
                    .map(|index| select_properties(&data_property, &(index as usize)))
                    .collect::<Result<Vec<ValueProperties>>>(),
                ArrayND::Bool(mask) => to_name_vec(&mask)?.into_iter()
                    .enumerate().filter(|(_, mask)| *mask)
                    .map(|(idx, _)| select_properties(&data_property, &idx))
                    .collect::<Result<Vec<ValueProperties>>>(),
                _ => return Err("the data type of the indices are not supported".into())
            },
            ValueProperties::Vector2DJagged(_) => Err("indexing is not supported on vectors".into())
        }?;

        stack_properties(&properties)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


pub fn to_name_vec<T: Clone>(columns: &ArrayD<T>) -> Result<Vec<T>> {
    match columns.ndim().clone() {
        0 => Ok(vec![columns.first().ok_or::<Error>("At least one column name must be supplied".into())?.clone()]),
        1 => match columns.clone().into_dimensionality::<Ix1>() {
            Ok(columns) => Ok(columns.to_vec()),
            Err(_) => Err("column names must be 1-dimensional".into())
        },
        _ => Err("dimensionality of column names must be less than 2".into())
    }
}

pub fn mask_columns<T: Clone>(column_names: &Vec<T>, mask: &Vec<bool>) -> Result<Vec<T>> {
    if mask.len() != column_names.len() {
        return Err("boolean mask must be the same length as the column names".into());
    }
    Ok(column_names.iter().zip(mask)
        .filter(|(_, mask)| **mask)
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<T>>())
}

fn take<T: Clone>(vector: &Vec<T>, index: &usize) -> Result<T> {
    match vector.get(*index) {
        Some(value) => Ok(value.clone()),
        None => Err("property column index is out of bounds".into())
    }
}

fn select_properties(properties: &ArrayNDProperties, index: &usize) -> Result<ValueProperties> {
    let mut properties = properties.clone();
    properties.c_stability = vec![take(&properties.c_stability, index)?];
    properties.num_columns = Some(1);
    if let Some(nature) = &properties.nature {
        properties.nature = Some(match nature {
            Nature::Continuous(continuous) => Nature::Continuous(NatureContinuous {
                min: match &continuous.min {
                    Vector1DNull::F64(min) => Vector1DNull::F64(vec![take(min, index)?]),
                    Vector1DNull::I64(min) => Vector1DNull::I64(vec![take(min, index)?]),
                    _ => return Err("min must be numeric".into())
                },
                max: match &continuous.max {
                    Vector1DNull::F64(max) => Vector1DNull::F64(vec![take(max, index)?]),
                    Vector1DNull::I64(max) => Vector1DNull::I64(vec![take(max, index)?]),
                    _ => return Err("max must be numeric".into())
                },
            }),
            Nature::Categorical(categorical) => Nature::Categorical(NatureCategorical {
                categories: match &categorical.categories {
                    Vector2DJagged::F64(cats) => Vector2DJagged::F64(vec![take(&cats, index)?]),
                    Vector2DJagged::I64(cats) => Vector2DJagged::I64(vec![take(&cats, index)?]),
                    Vector2DJagged::Bool(cats) => Vector2DJagged::Bool(vec![take(&cats, index)?]),
                    Vector2DJagged::Str(cats) => Vector2DJagged::Str(vec![take(&cats, index)?]),
                }
            })
        })
    }
    Ok(ValueProperties::ArrayND(properties))
}

fn get_common_value<T: Clone + Eq>(values: &Vec<T>) -> Option<T> {
    match values.windows(2).all(|w| w[0] == w[1]) {
        true => values.first().cloned(), false => None
    }
}

fn stack_properties(all_properties: &Vec<ValueProperties>) -> Result<ValueProperties> {
    let all_properties = all_properties.into_iter()
        .map(|property| Ok(property.get_arraynd()?.clone()))
        .collect::<Result<Vec<ArrayNDProperties>>>()?;

    let num_records = get_common_value(&all_properties.iter()
        .map(|prop| prop.num_records).collect()).unwrap_or(None);
    let dataset_id = get_common_value(&all_properties.iter()
        .map(|prop| prop.dataset_id).collect()).unwrap_or(None);

    if num_records.is_none() && dataset_id.is_none() {
        return Err("dataset may not be conformable".into())
    }

    if all_properties.iter().any(|prop| prop.aggregator.is_some()) {
        return Err("indexing is not currently supported on aggregated data".into())
    }

    // TODO: preserve nature when indexing

    Ok(ValueProperties::ArrayND(ArrayNDProperties {
        num_records,
        num_columns: all_properties.iter()
            .map(|prop| prop.num_columns)
            .fold(Some(0), |total, num| match (total, num) {
                (Some(total), Some(num)) => Some(total + num),
                _ => None
            }),
        nullity: get_common_value(&all_properties.iter().map(|prop| prop.nullity).collect()).unwrap_or(true),
        releasable: get_common_value(&all_properties.iter().map(|prop| prop.releasable).collect()).unwrap_or(true),
        c_stability: all_properties.iter().flat_map(|prop| prop.c_stability.clone()).collect(),
        aggregator: None,
        nature: None,
        data_type: get_common_value(&all_properties.iter().map(|prop| prop.data_type.clone()).collect())
            .ok_or::<Error>("dataset must have homogeneous type".into())?,
        dataset_id
    }))
}