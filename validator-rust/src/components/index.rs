use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Array, Value, ValueProperties, Indexmap, ArrayProperties, Nature, NatureContinuous, NatureCategorical, Vector1DNull, Jagged};

use crate::{proto, base};
use crate::components::{Component, Named};

use std::ops::Deref;
use ndarray::ArrayD;
use ndarray::prelude::*;
use crate::utilities::get_common_value;

impl Component for proto::Index {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.clone();

        let column_names = public_arguments.get("columns")
            .ok_or_else(|| Error::from("columns: missing"))?.deref().to_owned().array()?.clone();

        let dimensionality = Some(column_names.shape().len() as i64 + 1);

        let properties = match data_property {
            ValueProperties::Indexmap(data_property) => {
                // TODO: Should columnar stacking of partitions be allowed?
                data_property.assert_is_dataframe()?;

                match data_property.properties {
                    Indexmap::Str(value_properties) => match column_names {
                        // String column names on string indexmap
                        Array::Str(column_names) => to_name_vec(&column_names)?.into_iter()
                            .map(|v| value_properties.get(&v).cloned())
                            .collect::<Option<Vec<ValueProperties>>>()
                            .ok_or_else(|| Error::from("columns: unknown column in index")),
                        // Bool mask on string indexmap
                        Array::Bool(column_names) => {
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
                        // Indices on string indexmap
                        Array::I64(indices) => {
                            let indices = to_name_vec(&indices)?;
                            let column_names = value_properties.keys().cloned().collect::<Vec<String>>();
                            indices.iter().map(|index| value_properties.get(column_names.get(*index as usize)
                                .ok_or_else(|| Error::from("column index is out of range"))?).cloned()
                                .ok_or_else(|| Error::from("properties not found"))).collect::<Result<Vec<ValueProperties>>>()
                        },
                        Array::F64(_) => Err("columns may not have float type".into())
                    },
                    Indexmap::I64(value_properties) => match column_names {
                        // I64 column names on I64 indexmap
                        Array::I64(column_names) => to_name_vec(&column_names)?.into_iter()
                            .map(|v| value_properties.get(&v).cloned())
                            .collect::<Option<Vec<ValueProperties>>>()
                            .ok_or_else(|| Error::from("columns: unknown column in index")),
                        // Bool mask on I64 indexmap
                        Array::Bool(column_names) => {
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
                    Indexmap::Bool(value_properties) =>
                        to_name_vec(column_names.bool()?)?.into_iter()
                            .map(|name| value_properties.get(&name).cloned()
                                .ok_or_else(|| Error::from("columns: unknown column in index")))
                            .collect::<Result<Vec<ValueProperties>>>()
                }
            },
            ValueProperties::Array(data_property) => {

                if !data_property.releasable {
                    data_property.assert_is_not_aggregated()?;
                }

                match column_names {
                    Array::I64(indices) => to_name_vec(&indices)?.into_iter()
                        .map(|index| select_properties(&data_property, &(index as usize)))
                        .collect::<Result<Vec<ValueProperties>>>(),
                    Array::Bool(mask) => to_name_vec(&mask)?.into_iter()
                        .enumerate().filter(|(_, mask)| *mask)
                        .map(|(idx, _)| select_properties(&data_property, &idx))
                        .collect::<Result<Vec<ValueProperties>>>(),
                    _ => return Err("when indexing an array, the data type of the indices must be integer column number(s) or a boolean mask".into())
                }
            },
            ValueProperties::Jagged(_) => Err("indexing is not supported on vectors".into()),
            ValueProperties::Function(_) => Err("indexing is not suppported on functions".into())
        }?;

        stack_properties(&properties, dimensionality)
    }

}

impl Named for proto::Index {
    fn get_names(
        &self,
        public_arguments: &HashMap<String, Value>,
        argument_variables: &HashMap<String, Vec<String>>,
        _release: Option<&Value>
    ) -> Result<Vec<String>> {
        let input_names = argument_variables.get("data").ok_or("data: missing")?;
        Ok(match public_arguments.get("columns")
            .ok_or_else(|| Error::from("columns: missing"))?.to_owned()
            .array()? {
            Array::Str(names) =>
                names.iter().cloned().collect::<Vec<String>>(),
            Array::I64(indices) => indices.iter()
                .map(|idx| input_names.get(*idx as usize).cloned())
                .collect::<Option<Vec<String>>>()
                .ok_or_else(|| "attempted to retrieve an out-of-bounds name")?,
            Array::Bool(mask) => mask.iter()
                .zip(input_names.iter())
                .filter(|(mask, _)| **mask)
                .map(|(_, name)| name.clone())
                .collect::<Vec<String>>(),
            _ => return Err("column names may not be floats".into())
        })
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

pub fn mask_columns<T: Clone>(column_names: &[T], mask: &[bool]) -> Result<Vec<T>> {
    if mask.len() != column_names.len() {
        return Err("boolean mask must be the same length as the column names".into());
    }
    Ok(column_names.iter().zip(mask)
        .filter(|(_, mask)| **mask)
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<T>>())
}

fn take<T: Clone>(vector: &[T], index: &usize) -> Result<T> {
    match vector.get(*index) {
        Some(value) => Ok(value.clone()),
        None => Err("property column index is out of bounds".into())
    }
}

fn select_properties(properties: &ArrayProperties, index: &usize) -> Result<ValueProperties> {
    let mut properties = properties.clone();
    properties.c_stability = vec![take(&properties.c_stability, index)?];
    properties.num_columns = Some(1);
    if let Some(nature) = &properties.nature {
        properties.nature = Some(match nature {
            Nature::Continuous(continuous) => Nature::Continuous(NatureContinuous {
                lower: match &continuous.lower {
                    Vector1DNull::F64(lower) => Vector1DNull::F64(vec![take(lower, index)?]),
                    Vector1DNull::I64(lower) => Vector1DNull::I64(vec![take(lower, index)?]),
                    _ => return Err("lower must be numeric".into())
                },
                upper: match &continuous.upper {
                    Vector1DNull::F64(upper) => Vector1DNull::F64(vec![take(upper, index)?]),
                    Vector1DNull::I64(upper) => Vector1DNull::I64(vec![take(upper, index)?]),
                    _ => return Err("upper must be numeric".into())
                },
            }),
            Nature::Categorical(categorical) => Nature::Categorical(NatureCategorical {
                categories: match &categorical.categories {
                    Jagged::F64(cats) => Jagged::F64(vec![take(&cats, index)?]),
                    Jagged::I64(cats) => Jagged::I64(vec![take(&cats, index)?]),
                    Jagged::Bool(cats) => Jagged::Bool(vec![take(&cats, index)?]),
                    Jagged::Str(cats) => Jagged::Str(vec![take(&cats, index)?]),
                }
            })
        })
    }
    Ok(ValueProperties::Array(properties))
}

fn stack_properties(all_properties: &Vec<ValueProperties>, dimensionality: Option<i64>) -> Result<ValueProperties> {
    let all_properties = all_properties.into_iter()
        .map(|property| Ok(property.array()?.clone()))
        .collect::<Result<Vec<ArrayProperties>>>()?;

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

    Ok(ValueProperties::Array(ArrayProperties {
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
            .ok_or_else(|| Error::from("dataset must have homogeneous type"))?,
        dataset_id: all_properties[0].dataset_id,
        // this is a library-wide assumption - that datasets have more than zero rows
        is_not_empty: true,
        dimensionality
    }))
}